// components/fpsm/fpsm_flicker.cpp
//
// Surgical updates:
//  - Use cfg-based ulp_lp_core_run() to set LP timer cadence
//  - Pre-init RTC GPIO so LP owns the pad cleanly
//  - Clear any RTC holds during HP↔LP handoff
//  - Leave all other behavior intact

#include "fpsm_flicker.h"

#include <inttypes.h>
#include "esp_log.h"
#include "driver/gpio.h"
#include "driver/rtc_io.h"     // NEW: RTC pad pre-init / hold control
#include "ulp_lp_core.h"       // NEW: brings in ulp_lp_core_cfg_t and run(cfg)
#include "driver/ledc.h"
#include "esp_sleep.h"

// Prebuilt LP blob you already placed at components/fpsm/ulp/
#include "ulp/lp_core_bin.h"

// --------- Configuration you already use ----------
static const char *TAG = "fpsm";
static constexpr gpio_num_t FPSM_GPIO = GPIO_NUM_1;   // HP pad the MOSFET gate is wired to

// ULP shared vars: hard-coded addresses from your nm dump
static volatile uint32_t *const ULP_RUN_FLAG        = reinterpret_cast<volatile uint32_t *>(0x500008a0);
static volatile uint32_t *const ULP_BASE_BRIGHTNESS = reinterpret_cast<volatile uint32_t *>(0x5000089c);
// NEW tunables (placeholders — replace with your nm addresses)
static volatile uint32_t *const ULP_CFG_ENV_CYCLES     = reinterpret_cast<volatile uint32_t *>(0x50000894); // ulp_ulp_cfg_env_cycles
static volatile uint32_t *const ULP_CFG_FRAME_PAUSE    = reinterpret_cast<volatile uint32_t *>(0x50000890); // ulp_ulp_cfg_frame_pause
static volatile uint32_t *const ULP_CFG_SHIMMER_AMP    = reinterpret_cast<volatile uint32_t *>(0x5000088c); // ulp_ulp_cfg_shimmer_amp
static volatile uint32_t *const ULP_CFG_JITTER_HOLD    = reinterpret_cast<volatile uint32_t *>(0x50000888); // ulp_ulp_cfg_jitter_hold
static volatile uint32_t *const ULP_CFG_DRIFT_DIV      = reinterpret_cast<volatile uint32_t *>(0x50000884); // ulp_ulp_cfg_drift_div
static volatile uint32_t *const ULP_CFG_BREATH_SMOOTH  = reinterpret_cast<volatile uint32_t *>(0x50000880); // ulp_ulp_cfg_breath_smooth
static volatile uint32_t *const ULP_CFG_LVL_SMOOTH     = reinterpret_cast<volatile uint32_t *>(0x5000087c); // ulp_ulp_cfg_drift_div
static volatile uint32_t *const ULP_CFG_TARGET_STRIDE  = reinterpret_cast<volatile uint32_t *>(0x50000878); // ulp_ulp_cfg_breath_smooth


// Internal state
static bool s_lp_loaded  = false;
static bool s_lp_running = false;

// Small helpers
static inline void lp_mem_barrier() { __asm__ __volatile__("" ::: "memory"); }

static void dump_ulps(const char *ctx) {
  ESP_LOGD(TAG, "[%s] ULP vars: run_flag=%" PRIu32 " base_brightness=%" PRIu32,
           ctx ? ctx : "-", *ULP_RUN_FLAG, *ULP_BASE_BRIGHTNESS);
}

static void log_gpio_dir_level(const char *ctx) {
  int lvl = gpio_get_level(FPSM_GPIO);
  gpio_mode_t mode;
  // gpio_get_direction() doesn’t exist for all chips; infer by trying to set and read back output enable bit
  // For visibility only: we’ll print “(dir=OUT or IN)”
  // We’ll conservatively say OUT if we can drive high then read back high.
  gpio_set_direction(FPSM_GPIO, GPIO_MODE_OUTPUT);
  gpio_set_level(FPSM_GPIO, 1);
  int after = gpio_get_level(FPSM_GPIO);
  // Put it back to input (we don’t keep this)
  gpio_set_direction(FPSM_GPIO, GPIO_MODE_INPUT);
  mode = (after == 1) ? GPIO_MODE_OUTPUT : GPIO_MODE_INPUT;
  ESP_LOGD(TAG, "[%s] GPIO1 level=%d dir=%s", ctx ? ctx : "-", lvl, (mode == GPIO_MODE_OUTPUT) ? "OUT" : "IN");
}

// ---------------- HP<->LP GPIO handoff ----------------

// Give LP a high-Z pad to drive.
static void release_hp_pad_for_lp(gpio_num_t hp_gpio) {
  // Make sure no retention/hold is active and HP isn’t driving anything
  ledc_stop(LEDC_LOW_SPEED_MODE, LEDC_CHANNEL_0, /*idle_level=*/0);  // LOW
  gpio_set_direction(hp_gpio, GPIO_MODE_OUTPUT);
  gpio_set_level(hp_gpio, 0);
  
  gpio_hold_dis(hp_gpio);
  rtc_gpio_hold_dis(hp_gpio);
  gpio_sleep_sel_dis(hp_gpio);     // harmless if not set
  rtc_gpio_init(hp_gpio);

  ESP_LOGI(TAG, "Released HP control of GPIO%d for LP core.", (int)hp_gpio);
  log_gpio_dir_level("release");
}

// Take the pad back for HP (simple push-pull OUT, low).
static void reclaim_hp_pad(gpio_num_t hp_gpio) {
  // Ensure any RTC hold is cleared before reconfiguring
  gpio_hold_dis(hp_gpio);
  rtc_gpio_hold_dis(hp_gpio);
  gpio_sleep_sel_dis(hp_gpio);
  
  rtc_gpio_set_level(hp_gpio, 0);                       // optional: release low
  rtc_gpio_set_direction(hp_gpio, RTC_GPIO_MODE_DISABLED);
  rtc_gpio_deinit(hp_gpio);
  rtc_gpio_force_hold_dis_all();
  
  gpio_dump_io_configuration(stdout, 1ULL << FPSM_GPIO);

  ESP_LOGI(TAG, "Reclaimed HP control of GPIO%d.", (int)hp_gpio);
  log_gpio_dir_level("reclaim");
}

// Ensure RTC subsystem has seen this pad at least once from HP.
// Espressif recommends rtc_gpio_init() be called on any IO the LP app will drive.
static void fpsm_preinit_rtc_gpio_for_lp(gpio_num_t hp_gpio) {
  rtc_gpio_init(hp_gpio);
  rtc_gpio_hold_dis(hp_gpio);
}

static void hp_ledc_rebind_gpio1_ch0() {
  // Read the current duty so we can keep whatever HP last set (0..2^res-1)
  uint32_t duty = ledc_get_duty(LEDC_LOW_SPEED_MODE, LEDC_CHANNEL_0);

  ledc_channel_config_t ch = {};
  ch.gpio_num   = GPIO_NUM_1;            // HP light pin
  ch.speed_mode = LEDC_LOW_SPEED_MODE;   // ESPHome uses low-speed on C6
  ch.channel    = LEDC_CHANNEL_0;        // From your logs: Channel 0
  ch.intr_type  = LEDC_INTR_DISABLE;
  ch.timer_sel  = LEDC_TIMER_0;          // ESPHome's default timer for ch0
  ch.duty       = duty;                  // keep last duty value
  ch.hpoint     = 0;

  // This call re-applies the GPIO matrix routing LEDC->GPIO1
  (void) ledc_channel_config(&ch);

  // Ensure duty takes effect (some IDF versions require an explicit update)
  ledc_update_duty(LEDC_LOW_SPEED_MODE, LEDC_CHANNEL_0);
}

// ------------------- Public API ----------------------

bool fpsm_init() {
  if (s_lp_loaded) return true;
  
  if (*ULP_RUN_FLAG == 1u) {
    s_lp_loaded  = true;
    s_lp_running = true;
    ESP_LOGI(TAG, "fpsm_init: LP flame already running; skipping binary load & setup.");
    dump_ulps("init-preserve");  // keep your existing debug
    return true;
  }

  const uint8_t *bin    = lp_fpsm_c6_bin;
  const size_t   binlen = (size_t) lp_fpsm_c6_bin_len;

  if (!bin || binlen == 0) {
    ESP_LOGE(TAG, "No LP binary linked (lp_fpsm_c6_bin is null/empty).");
    return false;
  }

  // NEW: pre-initialize RTC GPIO so LP can own the pad cleanly
  fpsm_preinit_rtc_gpio_for_lp(FPSM_GPIO);

  // Load ULP program into LP memory
  if (ulp_lp_core_load_binary(bin, binlen) != ESP_OK) {
    ESP_LOGE(TAG, "ulp_lp_core_load_binary() failed.");
    return false;
  }

  // Initialize shared control variables (safe defaults)
  *ULP_RUN_FLAG        = 0u;     // disabled until start()
  *ULP_BASE_BRIGHTNESS = 0u;   // mid brightness default
  
    // Seed config defaults once if zero (runtime-tunable later)
  *ULP_CFG_ENV_CYCLES    = 110u;     // 20..200
  *ULP_CFG_FRAME_PAUSE   = 0u;  // cycles between envelopes
  *ULP_CFG_SHIMMER_AMP   = 10u;     // 0..32
  *ULP_CFG_JITTER_HOLD   = 14u;      // 1..64
  *ULP_CFG_DRIFT_DIV     = 4u;      // 1..6 (bigger=slower)
  *ULP_CFG_BREATH_SMOOTH = 24u;     // 8..64
  *ULP_CFG_LVL_SMOOTH    = 24u;     // 8..64 (higher = smoother, slower)
  *ULP_CFG_TARGET_STRIDE = 4u;     // 1..64 (lower = more responsive)
  
  lp_mem_barrier();
  dump_ulps("init");

  s_lp_loaded = true;
  ESP_LOGI(TAG, "LP binary loaded (%u bytes).", (unsigned) binlen);
  return true;
}

bool fpsm_start() {
  if (!s_lp_loaded && !fpsm_init()) return false;

  // IMPORTANT: keep LEDC (HP PWM) from owning the pin while LP is active.
  // Your YAML also turns the light off before enabling FPSM; this is belt-and-suspenders.
  release_hp_pad_for_lp(FPSM_GPIO);
  
  //Enable LP During Deep Sleep
  esp_sleep_pd_config(ESP_PD_DOMAIN_RTC_PERIPH,    ESP_PD_OPTION_ON);
  esp_sleep_pd_config(ESP_PD_DOMAIN_RC_FAST,  ESP_PD_OPTION_ON);
  

  *ULP_RUN_FLAG = 1u;   // tell LP firmware to run the flicker loop
  lp_mem_barrier();
  dump_ulps("start-before-run");

  // Run LP core with a timer wake ~12ms (≈83 Hz) for smooth flicker frames
  ulp_lp_core_cfg_t cfg = {};
  cfg.wakeup_source = ULP_LP_CORE_WAKEUP_SOURCE_LP_TIMER;
  cfg.lp_timer_sleep_duration_us = 12000;

  if (ulp_lp_core_run(&cfg) != ESP_OK) {
    ESP_LOGE(TAG, "ulp_lp_core_run(cfg) failed.");
    return false;
  }
  s_lp_running = true;
  ESP_LOGI(TAG, "LP core running.");
  return true;
}

void fpsm_stop() {
  if (!s_lp_loaded) return;

  // Ask LP to stop flicker
  *ULP_RUN_FLAG = 0u;
  lp_mem_barrier();
  dump_ulps("stop-before-halt");

  // Halt LP and give HP the pad back
  ulp_lp_core_stop();
  s_lp_running = false;
  reclaim_hp_pad(FPSM_GPIO);
  hp_ledc_rebind_gpio1_ch0();
  
  //Revert Sleep Control Back to Auto
  esp_sleep_pd_config(ESP_PD_DOMAIN_RTC_PERIPH,    ESP_PD_OPTION_AUTO);
  esp_sleep_pd_config(ESP_PD_DOMAIN_RC_FAST,  ESP_PD_OPTION_AUTO);
  
  // NEW: force HP PWM to a true 0 duty right after rebind to avoid a blip
  ledc_set_duty(LEDC_LOW_SPEED_MODE, LEDC_CHANNEL_0, 0);
  ledc_update_duty(LEDC_LOW_SPEED_MODE, LEDC_CHANNEL_0);
  gpio_set_level(FPSM_GPIO, 0); 
  
}

void fpsm_set_enabled(bool en) {
  if (!s_lp_loaded && !fpsm_init()) return;

  if (en) {
    fpsm_start();
  } else {
    fpsm_stop();
  }
  ESP_LOGD(TAG, "FPSM enable -> %s", en ? "ON" : "OFF");
  dump_ulps("set_enabled");
}

void fpsm_set_brightness(uint8_t level) {
  if (!s_lp_loaded && !fpsm_init()) return;
  *ULP_BASE_BRIGHTNESS = (uint32_t) level;
  lp_mem_barrier();
  ESP_LOGD(TAG, "FPSM brightness -> %u", (unsigned) level);
  dump_ulps("set_brightness");
}

// -------------- NEW: public setters to tune at runtime --------------
bool fpsm_is_running() {
  return s_lp_running;
}

void fpsm_set_env_cycles(uint8_t env_cycles) {
  if (!s_lp_loaded && !fpsm_init()) return;
  uint32_t v = env_cycles; if (v < 20) v = 20; if (v > 200) v = 200;
  *ULP_CFG_ENV_CYCLES = v; lp_mem_barrier();
  ESP_LOGD(TAG, "FPSM env_cycles -> %u", (unsigned) v);
}

void fpsm_set_frame_pause(uint32_t cycles) {
  if (!s_lp_loaded && !fpsm_init()) return;
  if (cycles > 200000u) cycles = 200000u;
  *ULP_CFG_FRAME_PAUSE = cycles; lp_mem_barrier();
  ESP_LOGD(TAG, "FPSM frame_pause -> %u cycles", (unsigned) cycles);
}

void fpsm_set_shimmer_amp(uint8_t amp) {
  if (!s_lp_loaded && !fpsm_init()) return;
  uint32_t v = (amp > 32) ? 32 : amp;
  *ULP_CFG_SHIMMER_AMP = v; lp_mem_barrier();
  ESP_LOGD(TAG, "FPSM shimmer_amp -> %u", (unsigned) v);
}

void fpsm_set_jitter_hold(uint8_t hold) {
  if (!s_lp_loaded && !fpsm_init()) return;
  uint32_t v = hold; if (v < 1) v = 1; if (v > 64) v = 64;
  *ULP_CFG_JITTER_HOLD = v; lp_mem_barrier();
  ESP_LOGD(TAG, "FPSM jitter_hold -> %u", (unsigned) v);
}

void fpsm_set_drift_div(uint8_t div) {
  if (!s_lp_loaded && !fpsm_init()) return;
  uint32_t v = div; if (v < 1) v = 1; if (v > 6) v = 6;
  *ULP_CFG_DRIFT_DIV = v; lp_mem_barrier();
  ESP_LOGD(TAG, "FPSM drift_div -> %u", (unsigned) v);
}

void fpsm_set_breath_smooth(uint8_t smooth) {
  if (!s_lp_loaded && !fpsm_init()) return;
  uint32_t v = smooth; if (v < 8) v = 8; if (v > 64) v = 64;
  *ULP_CFG_BREATH_SMOOTH = v; lp_mem_barrier();
  ESP_LOGD(TAG, "FPSM breath_smooth -> %u", (unsigned) v);
}

void fpsm_set_lvl_smooth(uint8_t smooth) {
  if (!s_lp_loaded && !fpsm_init()) return;
  uint32_t v = smooth; if (v < 8) v = 8; if (v > 64) v = 64;
  *ULP_CFG_LVL_SMOOTH = v; lp_mem_barrier();
  ESP_LOGD(TAG, "FPSM lvl_smooth -> %u", (unsigned)v);
}

void fpsm_set_target_stride(uint8_t stride) {
  if (!s_lp_loaded && !fpsm_init()) return;
  uint32_t v = stride; if (v < 1) v = 1; if (v > 64) v = 64;
  *ULP_CFG_TARGET_STRIDE = v; lp_mem_barrier();
  ESP_LOGD(TAG, "FPSM target_stride -> %u", (unsigned)v);
}

void fpsm_apply_preset_calm_pillar_v3() {
  // Smooth, slow, pillar candle feel
  fpsm_set_env_cycles(110);
  fpsm_set_frame_pause(0);
  fpsm_set_shimmer_amp(10);
  fpsm_set_jitter_hold(14);
  fpsm_set_drift_div(4);
  fpsm_set_breath_smooth(24);
  fpsm_set_lvl_smooth(24);
  fpsm_set_target_stride(4);
}

void fpsm_apply_preset_standard_candle_v3() {
  // Natural candle with some life, still smooth
  fpsm_set_env_cycles(80);
  fpsm_set_frame_pause(0);
  fpsm_set_shimmer_amp(14);
  fpsm_set_jitter_hold(10);
  fpsm_set_drift_div(3);
  fpsm_set_breath_smooth(18);
  fpsm_set_lvl_smooth(18);
  fpsm_set_target_stride(3);
}

void fpsm_apply_preset_fireplace_v3() {
  // Livelier "gusty" flame but keep it non-strobe
  fpsm_set_env_cycles(65);    // faster envelope
  fpsm_set_frame_pause(0);
  fpsm_set_shimmer_amp(20);   // stronger sparkle
  fpsm_set_jitter_hold(8);    // less hold → more micro motion
  fpsm_set_drift_div(2);      // quicker baseline sway
  fpsm_set_breath_smooth(16); // quicker breath
  fpsm_set_lvl_smooth(16);    // more responsive output
  fpsm_set_target_stride(2);  // refresh target more often
}

void fpsm_apply_preset_nightlight_glow_v3() {
  // Very subtle, soothing glow
  fpsm_set_env_cycles(125);
  fpsm_set_frame_pause(0);
  fpsm_set_shimmer_amp(8);
  fpsm_set_jitter_hold(16);
  fpsm_set_drift_div(4);
  fpsm_set_breath_smooth(28);
  fpsm_set_lvl_smooth(26);    // slightly less smoothing vs v3 (30)
  fpsm_set_target_stride(5);  // modest responsiveness
}