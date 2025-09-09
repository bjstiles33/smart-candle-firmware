// espnow_sync.h – ESPNOW-based rendezvous/link/wake for Smart Candle (ESP32)
// Implements: Hive peek (0.5s), Wake bursts, Link Mode with AP pre-scan,
// per-peer MAC tracking and unicast LINK_SET with ACKs.

#pragma once
#include "esphome.h"
#include "esphome/core/preferences.h"
#include <algorithm>
#include <cstring>
#include <cmath>
#include <map>  // For seq deduplication (std::map)
#include <array>
#include <vector>  // For mutable relay data copy
#include "esphome/components/light/light_state.h"
#include "esphome/components/select/select.h" 

//#include "components/fpsm/fpsm_flicker.h"
#include "driver/gpio.h"
#include "driver/ledc.h"

extern "C" {
  #include "esp_wifi.h"
  #include "esp_now.h"
  #include "esp_sleep.h"
}

//extern "C" {
//  void fpsm_apply_preset_calm_pillar_v3();
//  void fpsm_apply_preset_standard_candle_v3();
//  void fpsm_apply_preset_fireplace_v3();
//  void fpsm_apply_preset_nightlight_glow_v3();
//  void fpsm_set_enabled(bool en);
//}

using namespace esphome;

// ---------------- Packet Definitions and Enums ----------------
enum : uint8_t {
  PKT_STATE       = 0x01,
  PKT_WAKE        = 0x02,
  PKT_LINK_PROBE  = 0x10,
  PKT_LINK_RESP   = 0x11,
  PKT_LINK_SET    = 0x12,
  PKT_LINK_ACK    = 0x13,
  PKT_CMD         = 0x20
};

enum : uint8_t {
  CMD_SIGNAL          = 0x01, // Null Place Holder
  CMD_WAKE            = 0x02, // "Wake" equivalent, but via the new command packet
  CMD_SET_BRIGHTNESS  = 0x03, // example; arg0 = 0..255 (or whatever you choose)
  CMD_FPSM_ONOFF      = 0x04, // NEW: arg = 0(off) / 1(on)
  CMD_SET_PRESET      = 0x05  // NEW: arg = preset id (0..3)
  // 0x06..0x0F free for future commands
};

struct __attribute__((packed)) WakePkt   { uint8_t type, group; uint16_t ttl_s; uint32_t seq; uint32_t hive_time_ms; uint16_t crc; };
struct __attribute__((packed)) LinkProbe { uint8_t type, group; uint16_t ttl_s; uint32_t seq; uint32_t hive_time_ms; uint16_t crc; };
struct __attribute__((packed)) LinkResp  { uint8_t type, group; uint8_t home_ch; int8_t rssi; uint32_t seq; uint32_t hive_time_ms; uint16_t crc; };
struct __attribute__((packed)) LinkSet   { uint8_t type, group; uint8_t rc_ch; uint16_t used_mask; uint8_t leader_mac[6]; uint8_t queen_mac[6]; uint16_t ttl_s; uint32_t seq; uint32_t hive_time_ms; uint16_t crc; };
struct __attribute__((packed)) LinkAck   { uint8_t type, group; uint8_t rc_ch; uint8_t mac[6]; uint32_t seq; uint32_t hive_time_ms; uint16_t crc; };
struct __attribute__((packed)) CmdPkt    { uint8_t  type; uint8_t  group; uint8_t  cmd_id; uint8_t  arg; uint16_t ttl_s; uint32_t seq; uint32_t hive_time_ms; uint16_t crc; };

// ---------------- Global Helpers ----------------
namespace {
  static const uint8_t BCAST[6] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
  inline uint32_t now_ms() { return (uint32_t)millis(); }

  #if ESP_IDF_VERSION_MAJOR >= 5
    using RecvCbInfo = const esp_now_recv_info_t*;
    #define ESPNOW_RECV_SIG(info, data, len) static void recv_cb_(RecvCbInfo info, const uint8_t *data, int len)
    inline uint8_t rx_channel_from_info(RecvCbInfo info) { return (info && info->rx_ctrl) ? info->rx_ctrl->channel : 0; }
    inline int8_t rx_rssi_from_info(RecvCbInfo info) { return (info && info->rx_ctrl) ? info->rx_ctrl->rssi : 127; }
    inline const uint8_t* rx_mac_from_info(RecvCbInfo info) { return info ? info->src_addr : nullptr; }
  #else
    using RecvCbInfo = const uint8_t*;
    #define ESPNOW_RECV_SIG(info, data, len) static void recv_cb_(RecvCbInfo /*mac*/, const uint8_t *data, int len)
    inline uint8_t rx_channel_from_info(RecvCbInfo) { return 0; }
    inline int8_t rx_rssi_from_info(RecvCbInfo) { return 127; }
    inline const uint8_t* rx_mac_from_info(RecvCbInfo) { return nullptr; }
  #endif

  inline void wifi_set_auto_connect(bool en){ if(!en){ esp_wifi_clear_fast_connect(); esp_wifi_scan_stop(); esp_wifi_disconnect(); } }
  inline int mac_compare(const uint8_t a[6], const uint8_t b[6]){ return std::memcmp(a,b,6); }
} // namespace

class EspNowSync : public Component {
 public:
  // ---------------- Constants ----------------
  // Timing constants
  static constexpr uint32_t kWakeBurstPeriodMs = 180;
  static constexpr uint32_t kMinLinkMs = 15000;
  static constexpr uint32_t kProbeDwellMs = 120;
  static constexpr uint32_t kWakeBurstDelayMs = 3;
  static constexpr uint32_t kForcedDetachIntervalMs = 300;
  static constexpr uint32_t kHomePollIntervalMs = 2000;
  static constexpr uint32_t kRcSanityDelayMs = 800;
  static constexpr uint32_t kRespThrottleMs = 200;
  static constexpr uint32_t kEntryTripleTapSpacingMs = 45;  // spacing between the 3 starter micro-bursts
  static constexpr uint32_t kTtlExtendGuardMs = 5000;       // only extend TTL if incoming > remaining + 5s

  // Limit constants
  static constexpr int kWakeBurstFramesBase = 8;
  static constexpr int kMaxPeers = 16;
  static constexpr int kRespCapPerPhase = 10;

  // Power constants
  static constexpr float kTxBoostDbm = 19.5f;
  static constexpr float kTxBaseDbm = 11.0f;

  // Other constants
  static constexpr uint8_t kPrefsVersion = 2;

  // ---------------- Public: Singleton and Peer Structure ----------------
  /** Singleton accessor */
  static EspNowSync* get() { return self_; }

  struct Peer {
    uint8_t mac[6];
    uint8_t home_ch;
    bool added;
    volatile bool acked;
    uint8_t tries;
    int8_t rssi{-127};
  };

  // Constructor
  explicit EspNowSync(uint8_t group = 1) : group_(group & 0x0F) { self_ = this; }

  // ---------------- Public: ESPHome Lifecycle Methods ----------------
  /** Setup method for initialization */
  void setup() override {
    if (initialized_) return;
    select_external_antenna();
    wifi_mode_t mode{}; esp_wifi_get_mode(&mode); if(mode!=WIFI_MODE_STA && mode!=WIFI_MODE_APSTA) esp_wifi_set_mode(WIFI_MODE_STA);
    esp_wifi_start();
    // default to modem power-save and modest TX power; boost only during link/wake
    esp_wifi_set_ps(WIFI_PS_MIN_MODEM); set_tx_power_dbm_(kTxBaseDbm);
    if(esp_now_init()!=ESP_OK){ ESP_LOGE("espnow","esp_now_init failed"); return; }
    esp_now_register_recv_cb(&EspNowSync::recv_cb_);
    esp_now_register_send_cb(&EspNowSync::send_cb_);

    esp_now_peer_info_t p{}; std::memset(&p,0,sizeof(p)); std::memcpy(p.peer_addr,BCAST,6); p.ifidx=WIFI_IF_STA; p.channel=0; p.encrypt=false; esp_now_add_peer(&p);
    esp_wifi_get_mac(WIFI_IF_STA, self_mac_);

    pref_rc_   = global_preferences->make_preference<uint8_t>(0x45AA12C1);
    pref_mask_ = global_preferences->make_preference<uint16_t>(0x45AA12C2);
    pref_queen_mac_ = global_preferences->make_preference<uint8_t[6]>(0x45AA12C3);
    pref_rc_lock_ = global_preferences->make_preference<uint8_t>(0x45AA12C4);
    pref_prefs_version_ = global_preferences->make_preference<uint8_t>(0x45AA12C5);
    pref_boot_counter_  = global_preferences->make_preference<uint32_t>(0x45AA12C6);
    
    uint8_t lock_tmp = 1;                 // default: locked
    if (pref_rc_lock_.load(&lock_tmp)) {
      rc_locked_ = (lock_tmp != 0);
    } else {
      rc_locked_ = true;
    }
    
    diag_init_persistence_();
    
    uint8_t rc_tmp{0}; uint16_t mask_tmp{0}; uint8_t qmac_tmp[6]{}; if(pref_rc_.load(&rc_tmp) && rc_tmp>=1 && rc_tmp<=13) rc_ch_=rc_tmp; if(pref_mask_.load(&mask_tmp)) used_ch_mask_|=mask_tmp; if(pref_queen_mac_.load(qmac_tmp)) std::memcpy(queen_mac_, qmac_tmp, 6); 

    if (queen_mode_) { std::memcpy(queen_mac_, self_mac_, 6); pref_queen_mac_.save(queen_mac_); }
    
    if (used_ch_mask_ == 0) {
      uint16_t m = ap_mask_;
      if (home_ch_ >= 1 && home_ch_ <= 13) m |= (uint16_t)(1u << home_ch_);
      if (rc_ch_   >= 1 && rc_ch_   <= 13) m |= (uint16_t)(1u << rc_ch_);
      used_ch_mask_ = m;
      pref_mask_.save(&used_ch_mask_);
      ESP_LOGW("espnow", "Used-channel mask was empty at boot; set default=0x%04X", (unsigned)used_ch_mask_);
    }

    initialized_=true; ESP_LOGI("espnow","ESP-NOW ready (group=%u)", (unsigned)group_);
    if(queen_mode_) { time_synced_ = true; last_sync_ms_ = 0; } // Force initial NTP sync
  }

  /** Loop method for ongoing operations */
  void loop() override {
    if (is_peeking_) loop_peek_();
    if (is_waking_)  loop_wake_();
    if (is_cmd_beacon_) loop_cmd_beacon();
    if (link_phase_ != LinkPhase::IDLE) loop_link_();

    // Optional RC sanity ... (existing code unchanged)

    // NEW: perform deferred peer cleanup ~30s after link
    uint32_t t = now_ms();
    if (deferred_peer_clear_ms_ && t >= deferred_peer_clear_ms_) {
      clear_peers_();
      deferred_peer_clear_ms_ = 0;
    }

    if(queen_mode_ && t - last_sync_ms_ >= 1000) { sync_queen_time_(); }
  }

  // ---------------- Public: Queen Mode Management ----------------
  /** Set queen mode */
  void set_queen_mode(bool en) { queen_mode_ = en; if(en) { std::memcpy(queen_mac_, self_mac_, 6); pref_queen_mac_.save(queen_mac_); } }

  /** Start queen mode operations */
  void start_queen_mode() {
    esp_wifi_set_ps(WIFI_PS_NONE);
    set_tx_power_dbm_(kTxBoostDbm);
    ensure_wifi_connected();
  }

  /** Trigger wake for group */
  void trigger_wake_group(uint16_t ttl_s=300, uint32_t beacon_ms=45000) {
    start_wake_group(ttl_s, beacon_ms);
  }

  /** Check if in queen mode */
  bool is_queen() const { return queen_mode_; }

  // ---------------- Public: UI Binding and Control ----------------
  /** Bind UI light */
  inline void bind_ui_light(esphome::light::LightState *l) { ui_light_ = l; }

  /** Set UI brightness */
  inline void set_ui_brightness(float b, uint32_t transition_ms = 0) {
    if (!ui_light_) return;
    if (b < 0.0f) b = 0.0f;
    if (b > 1.0f) b = 1.0f;
    auto call = ui_light_->turn_on();
    call.set_brightness(b);
    call.set_transition_length((uint16_t) transition_ms);
    call.perform();
  }

  /** Set UI brightness from level */
  inline void set_ui_brightness_from_level(uint32_t level, uint32_t scale, uint32_t transition_ms = 0) {
    if (!ui_light_ || scale == 0U) return;
    const float b = std::min(1.0f, (float)level / (float)scale);
    set_ui_brightness(b, transition_ms);
  }    
  
  /** Bind saved brightness */
  inline void bind_saved_brightness(float *p) { saved_brightness_ptr_ = p; }

  /** Set saved brightness */
  inline void set_saved_brightness(float b) {
    if (!saved_brightness_ptr_) return;
    if (b < 0.0f) b = 0.0f;
    if (b > 1.0f) b = 1.0f;
    *saved_brightness_ptr_ = b;  // updates id(saved_brightness)
  }

  /** Set saved brightness from level */
  inline void set_saved_brightness_from_level(uint32_t level, uint32_t scale) {
    if (!saved_brightness_ptr_ || scale == 0U) return;
    float b = (float) level / (float) scale;
    if (b < 0.0f) b = 0.0f;
    if (b > 1.0f) b = 1.0f;
    *saved_brightness_ptr_ = b;
  }
  
  /** Bind preset select */
  inline void bind_preset_select(esphome::select::Select *sel) { preset_select_ = sel; }

  /** Set preset UI */
  inline void set_preset_ui(uint8_t preset_id) {
    if (!preset_select_) return;
    const char* name = preset_name_from_id_(preset_id);
    if (!name) return;
    auto call = preset_select_->make_call();
    call.set_option(name);
    call.perform();
  }

  // ---------------- Public: Command Beacon Management ----------------
  /** Start command beacon */
  void start_cmd_beacon(uint8_t cmd_id, uint8_t arg,
                        uint16_t ttl_s = 45, uint32_t beacon_ms = 12000) {
    refresh_home_ch_();
    uint8_t rc = (rc_ch_ >= 1 && rc_ch_ <= 13) ? rc_ch_ : home_ch_;

    esp_wifi_set_ps(WIFI_PS_NONE);
    set_tx_power_dbm_(kTxBoostDbm);

    wifi_set_auto_connect(false);
    ensure_detached_();
    set_channel_(rc);
    wifi_detached_ = true;

    cmd_id_   = cmd_id;
    cmd_arg_  = arg;
    cmd_ttl_s_ = ttl_s;
    is_cmd_beacon_ = true;
    cmd_end_ms_    = now_ms() + beacon_ms;
    cmd_channel_   = rc;
    cmd_next_forced_detach_ms_ = now_ms();

    cmd_entry_bursts_remaining_ = 3;
    cmd_entry_burst_last_ms_    = 0;
  }

  /** Loop for command beacon */
  void loop_cmd_beacon() {
    uint32_t t = now_ms();

    if (t >= cmd_next_forced_detach_ms_) {
      esp_wifi_scan_stop(); esp_wifi_disconnect();
      set_channel_(cmd_channel_);
      cmd_next_forced_detach_ms_ = t + kForcedDetachIntervalMs;
    }

    if (t >= cmd_end_ms_) {
      is_cmd_beacon_ = false;
      refresh_home_ch_();
      set_channel_(home_ch_);
      esp_wifi_set_ps(WIFI_PS_MIN_MODEM);
      set_tx_power_dbm_(kTxBaseDbm);
      wifi_set_auto_connect(true);
      esp_wifi_connect();
      wifi_detached_ = false;
      return;
    }

    // Triple-tap entry bursts
    if (cmd_entry_bursts_remaining_ > 0) {
      if (cmd_entry_burst_last_ms_ == 0 ||
          (t - cmd_entry_burst_last_ms_) >= kEntryTripleTapSpacingMs) {
        send_cmd_burst_(cmd_id_, cmd_arg_, cmd_ttl_s_);
        tx_wake_++;
        cmd_entry_burst_last_ms_ = t;
        cmd_entry_bursts_remaining_--;
      }
      return;
    }

    // Regular cadence
    send_cmd_burst_(cmd_id_, cmd_arg_, cmd_ttl_s_);
    tx_wake_++;
  }

  // ---------------- Public: Link Mode Management ----------------
  /** Start link mode */
  void start_link_mode(uint32_t peek_window_ms = 5000,
                       float     win            = 2.0f,
                       uint16_t  wake_ttl_s     = 300,
                       uint32_t  probe_ms       = 5000,
                       uint16_t  dwell_ms       = 35) {

    // Unlock RC while linking and update persisted lock flag
    rc_locked_ = false;
    uint8_t v = 0;
    pref_rc_lock_.save(&v);

    // Also allow local acceptance of SET for the duration of this run
    accept_set_until_ms_ = now_ms() + 60000U; // generous window; will be reset on finish

    if (!queen_mode_ && mac_compare(queen_mac_, BCAST) != 0 && mac_compare(queen_mac_, self_mac_) != 0) {
      ESP_LOGW("espnow", "Suggest running link mode on queen for better coverage");
    } else if (queen_mode_) {
      ESP_LOGI("espnow", "Queen leading link mode");
      win = 3.0f;
    }

    refresh_home_ch_();

    // Capture current SSID BEFORE we detach so the scan can be filtered
    wifi_ap_record_t cur{};
    uint8_t ssid_filter[33] = {0};
    if (esp_wifi_sta_get_ap_info(&cur) == ESP_OK && cur.ssid[0] != 0) {
      std::memcpy(ssid_filter, cur.ssid, 32);
    }

    // Build AP channel mask from a quick passive scan, filtered to our SSID
    wifi_set_auto_connect(false);
    ensure_detached_();

    // Performance mode during link: disable PS and bump TX power
    esp_wifi_set_ps(WIFI_PS_NONE);
    set_tx_power_dbm_(kTxBoostDbm);

    ap_mask_ = compute_ap_mask_(ssid_filter[0] ? ssid_filter : nullptr);
    if (ap_mask_ == 0) {
      if (home_ch_ >= 1 && home_ch_ <= 13) ap_mask_ |= (1u << home_ch_);
    }
    ESP_LOGD("espnow", "AP mask from scan: 0x%04X", ap_mask_);

    // Start with all visible AP channels in our dissemination mask
    used_ch_mask_ = ap_mask_;
    std::fill(std::begin(resp_hist_), std::end(resp_hist_), 0);
    peer_count_ = 0; // reset responders list

    // windows_to_cover (win) lets us scale coverage; floor keeps short but safe
    uint32_t floor_ms = std::max<uint32_t>(peek_window_ms + 1000U, 6000U);
    wake_per_ch_ms_   = std::max<uint32_t>((uint32_t)(peek_window_ms * win), floor_ms);
    probe_dwell_ms_   = (uint16_t)std::max<uint32_t>(probe_ms / 11U, 200U);

    // Initial dwell/channel
    link_ch_ = first_in_mask_(ap_mask_);
    set_channel_(link_ch_);
    wifi_detached_     = true;
    link_last_hop_ms_  = now_ms();
    link_last_burst_ms_= link_last_hop_ms_ - kWakeBurstPeriodMs;
    link_start_ms_     = link_last_hop_ms_;
    last_log_ms_       = link_start_ms_;
    link_hop_count_    = 1;
    link_curr_ch_      = link_ch_;

    // --- NEW: prime triple-tap for the first dwell ---
    entry_bursts_remaining_ = 3;
    entry_burst_last_ms_    = 0;

    // Compute end times for phases/guards
    uint32_t n = count_bits16_(ap_mask_);
    wake_sweep_end_ms_ = link_last_hop_ms_ + wake_per_ch_ms_ * n;
    link_end_ms_       = wake_sweep_end_ms_ + (uint32_t)probe_dwell_ms_ * n;
    guard_end_ms_      = link_end_ms_ + 3000U;

    // Activate link mode and initialize pacing/counters
    wake_ttl_s_              = wake_ttl_s;
    link_phase_              = LinkPhase::WAKE_SWEEP;
    last_probe_send_ms_      = 0;
    probe_sent_count_this_dwell_ = 0;
    resp_sent_this_phase_    = 0;         // reset WAKE-phase RESP pacing
    next_forced_detach_ms_   = now_ms();

    ESP_LOGI("espnow",
             "Link start: mask=0x%04X wake_per=%ums probe_per=%ums",
             ap_mask_, (unsigned)wake_per_ch_ms_, (unsigned)probe_dwell_ms_);
  }

  // ---------------- Public: Antenna Selection ----------------
  /** Select external antenna */
  void select_external_antenna() {
    gpio_config_t io{};
    io.mode = GPIO_MODE_OUTPUT;
    io.pin_bit_mask = (1ULL<<GPIO_NUM_3) | (1ULL<<GPIO_NUM_14);
    gpio_config(&io);
    gpio_set_level(GPIO_NUM_3, 0); // enable RF switch control
    gpio_set_level(GPIO_NUM_14, 1); // 1 = external antenna
  }

  // ---------------- Public: Hive Peek and Wake Management ----------------
  /** Start auto peek */
  void start_peek_auto(uint32_t window_ms=300){
    refresh_home_ch_(); uint8_t rc=(rc_ch_>=1&&rc_ch_<=13)?rc_ch_:home_ch_;
    // RX-only: disable PS during the short peek for max sensitivity, base TX power
    
    begin_peek_led_quiet_();
    
    esp_wifi_set_ps(WIFI_PS_NONE); set_tx_power_dbm_(kTxBaseDbm);
    wifi_set_auto_connect(false); ensure_detached_(); set_channel_(rc); wifi_detached_=true;
    peek_ch_=rc; next_forced_detach_peek_ms_=now_ms(); peek_end_ms_=now_ms()+window_ms; is_peeking_=true;
    
    //peek_skip_pad_handoff_ = !fpsm_is_running();
    //if (peek_skip_pad_handoff_) {
    //  // Stop the HP LEDC output and drive the pad low as a plain GPIO.
    //  // (Channel 0 is the default for a single LEDC output; if you’ve
    //  // customized the channel, adjust the channel index here.)
    //  ledc_stop(LEDC_LOW_SPEED_MODE, LEDC_CHANNEL_0, /*idle_level=*/0);
    //  gpio_set_direction(GPIO_NUM_1, GPIO_MODE_OUTPUT);
    //  gpio_set_level(GPIO_NUM_1, 0);
    //}
    
    // Added: Set wake window for better sleep/RX compatibility (docs recommend for station mode)
    esp_now_set_wake_window((uint16_t)window_ms);
  }

  /** Start wake group */
  void start_wake_group(uint16_t ttl_s = 45, uint32_t beacon_ms = 12000) {
    refresh_home_ch_(); 
    uint8_t rc = (rc_ch_ >= 1 && rc_ch_ <= 13) ? rc_ch_ : home_ch_;

    // TX-heavy: disable PS and boost TX power
    esp_wifi_set_ps(WIFI_PS_NONE);
    set_tx_power_dbm_(kTxBoostDbm);

    wifi_set_auto_connect(false);
    ensure_detached_();
    set_channel_(rc);
    wifi_detached_ = true;

    wake_ttl_s_   = ttl_s;
    is_waking_    = true;
    wake_end_ms_  = now_ms() + beacon_ms;
    wake_ch_      = rc;
    next_forced_detach_wake_ms_ = now_ms();

    // --- NEW: prime triple-tap for Wake-Now ---
    wake_entry_bursts_remaining_ = 3;
    wake_entry_burst_last_ms_    = 0;
  }

  // ---------------- Public: Channel and Link Diagnostics ----------------
  /** Get rendezvous channel */
  uint8_t rendezvous_channel() const { return rc_ch_; }

  /** Get home channel */
  uint8_t home_channel() const { return home_ch_; }

  /** Get used channel mask */
  uint16_t used_channel_mask() const { return used_ch_mask_; }

  /** Check if link is active */
  bool link_active() const { return link_phase_ != LinkPhase::IDLE; }

  /** Get hive awake ms remaining */
  uint32_t hive_awake_ms_remaining() const { uint32_t t=now_ms(); return hive_awake_until_>t?(hive_awake_until_-t):0; }

  /** Get AP mask */
  uint16_t ap_mask() const { return ap_mask_; }

  /** Copy queen MAC */
  void queen_mac(uint8_t out[6]) const { std::memcpy(out, queen_mac_, 6); }

  /** Get peer count */
  int peer_count() const { return peer_count_; }

  /** Check woke from timer boot */
  bool woke_from_timer_boot() const { return esp_sleep_get_wakeup_cause()==ESP_SLEEP_WAKEUP_TIMER; }

  /** Check if awake */
  bool is_awake() const { return hive_awake_ms_remaining()>0; }

  /** Ensure WiFi connected */
  void ensure_wifi_connected(){ refresh_home_ch_(); set_channel_(home_ch_); esp_wifi_connect(); wifi_detached_=false; }
  
  /** Copy last wake src MAC */
  void last_wake_src_mac(uint8_t out[6]) const { std::memcpy(out, last_wake_src_mac_, 6); }

  /** Get last wake time ms */
  uint32_t last_wake_time_ms() const { return last_wake_time_ms_; }

  /** Get last wake seq */
  uint32_t last_wake_seq() const { return last_wake_seq_; }

  /** Check RC locked */
  bool rc_locked() const { return rc_locked_; }
  
  /** Get last link complete s */
  uint32_t last_link_complete_s() const {
    return last_link_complete_ms_ ? ((now_ms() - last_link_complete_ms_) / 1000U) : 0U;  }
    
  /** Set hive interval ms */
  void set_hive_interval_ms(uint32_t ms) { hive_interval_ms_ = (ms < 1000U) ? 1000U : ms; }

  /** Get last rx rssi */
  int last_rx_rssi() const { return (int)last_rx_rssi_; }

  /** Get last rx type */
  uint8_t last_rx_type() const { return last_rx_type_; }

  /** Copy last rx MAC */
  void last_rx_mac(uint8_t out[6]) const { std::memcpy(out,last_rx_mac_,6); }

  /** Get link hop count */
  uint32_t link_hop_count() const { return link_hop_count_; }

  /** Get link current channel */
  uint8_t link_current_channel() const { return link_curr_ch_; }

  /** Get link phase string */
  const char* link_phase_str() const {
    switch (link_phase_) {
      case LinkPhase::WAKE_SWEEP: return "WAKE";
      case LinkPhase::PROBE: return "PROBE";
      default: return "IDLE";
    }
  }

  /** Get peers */
  const Peer* get_peers() const { return peers_; }

  /** Get peer count */
  int get_peer_count() const { return peer_count_; }

  /** Broadcast link set quick */
  void broadcast_link_set_quick_(){
    LinkSet set{}; set.type=PKT_LINK_SET; set.group=group_; set.rc_ch=rc_ch_; set.used_mask=used_ch_mask_;
    std::memcpy(set.leader_mac,self_mac_,6); std::memcpy(set.queen_mac, queen_mac_, 6); set.ttl_s = 1; set.seq=++seq_; set.hive_time_ms = current_hive_time(); 
    set.crc = compute_crc16_((uint8_t*)&set, sizeof(set) - sizeof(set.crc));
    for(uint8_t ch2: {rc_ch_, home_ch_}){
      set_channel_(ch2);
      send_burst_(BCAST, (uint8_t*)&set, sizeof(set), 3, 8);
    }
    tx_set_++;
  }

  // ---------------- Public: Count and Reset Diagnostics ----------------
  /** Get rx wake count */
  uint32_t rx_wake_count() const { return rx_wake_; }

  /** Get rx probe count */
  uint32_t rx_probe_count() const { return rx_probe_; }

  /** Get rx resp count */
  uint32_t rx_resp_count() const { return rx_resp_; }

  /** Get rx set count */
  uint32_t rx_set_count()  const { return rx_set_; }

  /** Get rx ack count */
  uint32_t rx_ack_count()  const { return rx_ack_; }

  /** Get tx wake count */
  uint32_t tx_wake_count() const { return tx_wake_; }

  /** Get tx resp count */
  uint32_t tx_resp_count() const { return tx_resp_; }

  /** Get tx set count */
  uint32_t tx_set_count()  const { return tx_set_; }

  /** Get tx ack count */
  uint32_t tx_ack_count()  const { return tx_ack_; }

  /** Reset diagnostics */
  void reset_diagnostics(){ rx_wake_=rx_probe_=rx_resp_=rx_set_=rx_ack_=0; tx_wake_=tx_resp_=tx_set_=tx_ack_=0; resp_sent_this_phase_=0; resp_sent_idle_=0; }

  /** Log diagnostics */
  void log_diagnostics(){
    char qbuf[18];
    sprintf(qbuf, "%02X:%02X:%02X:%02X:%02X:%02X", queen_mac_[0],queen_mac_[1],queen_mac_[2],queen_mac_[3],queen_mac_[4],queen_mac_[5]);
    ESP_LOGI("espnow","RC=%u, Home=%u, Used=0x%04X, LinkActive=%d, Phase=%s, Hops=%u, CurrCh=%u QueenMAC=%s",
             (unsigned)rc_ch_, (unsigned)home_ch_, (unsigned)used_ch_mask_, (int)link_active(),
             link_phase_str(), (unsigned)link_hop_count_, (unsigned)link_curr_ch_, qbuf);
    ESP_LOGI("espnow","Counts RX: wake=%u probe=%u resp=%u set=%u ack=%u; TX: wake=%u resp=%u set=%u ack=%u; peers=%d ap_mask=0x%04X",
             (unsigned)rx_wake_, (unsigned)rx_probe_, (unsigned)rx_resp_, (unsigned)rx_set_, (unsigned)rx_ack_,
             (unsigned)tx_wake_, (unsigned)tx_resp_, (unsigned)tx_set_, (unsigned)tx_ack_, peer_count_, (unsigned)ap_mask_);
    for(int i=0; i<peer_count_; i++){
      char mbuf[18];
      sprintf(mbuf, "%02X:%02X:%02X:%02X:%02X:%02X", peers_[i].mac[0],peers_[i].mac[1],peers_[i].mac[2],peers_[i].mac[3],peers_[i].mac[4],peers_[i].mac[5]);
      ESP_LOGI("espnow","Peer %d: MAC=%s home_ch=%u rssi=%d acked=%d", i, mbuf, (unsigned)peers_[i].home_ch, (int)peers_[i].rssi, (int)peers_[i].acked);
    }
  }

  // ---------------- Public: Time Sync Diagnostics ----------------
  /** Check if time synced */
  bool is_time_synced() const { return time_synced_; }

  /** Get last sync s */
  uint32_t last_sync_s() const { return (now_ms() - last_sync_ms_)/1000; }

  /** Get current hive time */
  uint32_t current_hive_time() const { return (uint32_t)((int64_t)now_ms() + time_offset_); }

  // ---------------- Public: RC Change Diagnostics ----------------
  /** Get last RC change reason */
  const char* last_rc_change_reason() const { return last_rc_change_reason_; }

  /** Get RC persist source */
  const char* rc_persist_source()   const { return rc_persist_source_; }

  /** Get RC change count */
  uint32_t    rc_change_count()     const { return rc_change_count_; }

  /** Get last RC change s */
  uint32_t    last_rc_change_s()    const { return last_rc_change_ms_ ? ((now_ms() - last_rc_change_ms_) / 1000U) : 0U; }

  /** Get boot count */
  uint32_t    boot_count()          const { return boot_counter_; }

 private:
  // ---------------- Private: Singleton and UI Bindings ----------------
  inline static EspNowSync* self_ = nullptr;
  
  esphome::light::LightState *ui_light_{nullptr};
  float *saved_brightness_ptr_{nullptr};
  esphome::select::Select *preset_select_{nullptr};

  // ---------------- Private: Group and Rendezvous State ----------------
  uint8_t group_{1};

  uint8_t rc_ch_{6};
  uint16_t used_ch_mask_{0};

  // ---------------- Private: Preferences and Diagnostic State ----------------
  ESPPreferenceObject pref_prefs_version_;
  ESPPreferenceObject pref_boot_counter_;

  uint32_t boot_counter_{0};
  uint8_t prefs_version_{0};
  bool prefs_version_ok_{false};
  char last_rc_change_reason_[24]{"N/A"};
  char rc_persist_source_[24]{"N/A"};
  uint32_t rc_change_count_{0};
  uint32_t last_rc_change_ms_{0};

  // ---------------- Private: Command Beacon State ----------------
  bool is_cmd_beacon_{false};
  uint32_t cmd_end_ms_{0};
  uint16_t cmd_ttl_s_{45};
  uint8_t cmd_channel_{0};
  uint32_t cmd_next_forced_detach_ms_{0};

  uint8_t cmd_entry_bursts_remaining_{0};
  uint32_t cmd_entry_burst_last_ms_{0};

  uint8_t cmd_id_{0};
  uint8_t cmd_arg_{0};

  // ---------------- Private: Hive and Peek State ----------------
  uint32_t hive_interval_ms_{15000U};
  bool peek_skip_pad_handoff_{false};
  bool quiet_hold_active_{false};

  bool is_waking_{false}; 
  uint32_t wake_end_ms_{0}; 
  uint16_t wake_ttl_s_{45};
  uint8_t wake_ch_{0}; 
  uint32_t next_forced_detach_wake_ms_{0};

  bool is_peeking_{false}; 
  uint32_t peek_end_ms_{0}; 
  uint8_t peek_ch_{0}; 
  uint32_t next_forced_detach_peek_ms_{0};
  
  uint32_t deferred_peer_clear_ms_{0};
  
  ESPPreferenceObject pref_rc_lock_;      // persisted RC lock flag
  bool rc_locked_{true};                  // default locked until first link
  uint32_t accept_set_until_ms_{0};       // arming window to accept LINK_SET
  uint32_t last_link_complete_ms_{0};     // optional diag

  // ---------------- Private: Link State and Phases ----------------
  enum class LinkPhase { IDLE, WAKE_SWEEP, PROBE };
  LinkPhase link_phase_{LinkPhase::IDLE}; 
  uint32_t link_start_ms_{0}; 
  uint32_t guard_end_ms_{0}; 
  uint32_t last_log_ms_{0};
  uint32_t link_end_ms_{0}; 
  uint32_t wake_sweep_end_ms_{0};
  uint32_t wake_per_ch_ms_{10000}; 
  uint16_t probe_dwell_ms_{kProbeDwellMs}; 
  uint8_t peer_quorum_{1};
  uint8_t link_ch_{1}; 
  uint32_t link_last_hop_ms_{0}; 
  uint32_t link_last_burst_ms_{0}; 
  uint32_t link_hop_count_{0}; 
  uint8_t link_curr_ch_{0};
  
  uint8_t last_wake_src_mac_[6]{};
  uint32_t last_wake_time_ms_{0};
  uint32_t last_wake_seq_{0};

  uint8_t entry_bursts_remaining_{0};
  uint32_t entry_burst_last_ms_{0};

  uint8_t wake_entry_bursts_remaining_{0};
  uint32_t wake_entry_burst_last_ms_{0};
  
  uint32_t last_probe_send_ms_{0};
  uint16_t probe_sent_count_this_dwell_{0};
  
  uint32_t last_resp_send_ms_{0};

  uint16_t ap_mask_{0};

  Peer peers_[kMaxPeers]; 
  int peer_count_{0};

  uint8_t home_ch_{1}; 
  uint32_t next_home_poll_ms_{0}; 
  uint32_t rc_sanity_due_ms_{0};

  bool wifi_detached_{false}; 
  uint32_t next_forced_detach_ms_{0};

  // ---------------- Private: Diagnostics and Counters ----------------
  volatile uint32_t hive_awake_until_{0}; 
  uint8_t last_rx_type_{0}; 
  int8_t last_rx_rssi_{127}; 
  uint8_t last_rx_mac_[6]{};
  uint32_t rx_wake_{0}, rx_probe_{0}, rx_resp_{0}, rx_set_{0}, rx_ack_{0};
  uint32_t tx_wake_{0}, tx_resp_{0}, tx_set_{0}, tx_ack_{0}; 
  uint32_t seq_{0}; 
  bool initialized_{false};

  ESPPreferenceObject pref_rc_; 
  ESPPreferenceObject pref_mask_; 
  ESPPreferenceObject pref_queen_mac_;

  uint8_t self_mac_[6]{};

  uint16_t resp_hist_[14]{};

  static constexpr int kDedupSlots = 24;
  struct DedupEntry {
    uint8_t mac[6]{};
    uint8_t type{0};
    uint32_t last_seq{0};
    uint32_t last_seen_ms{0};
  };
  DedupEntry dedup_[kDedupSlots]{};

  // ---------------- Private: Queen and Time Sync State ----------------
  bool queen_mode_{false};
  uint8_t queen_mac_[6]{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}; // Default BCAST until set

  uint8_t resp_sent_this_phase_{0};
  uint8_t resp_sent_idle_{0};

  int64_t time_offset_{0};
  uint32_t last_sync_ms_{0};
  bool time_synced_{false};

  // ---------------- Private: General Utility Helpers ----------------
  /** Begin peek LED quiet */
  void begin_peek_led_quiet_() {
    if (quiet_hold_active_) return;
    // If LP isn’t running, clamp GPIO1 low so any Wi-Fi detach or channel hop can’t blip the LED.
    if (!fpsm_is_running()) {
      // Stop LEDC output on ch0 and drive/pin-hold LOW (matches your LEDC ch & pin)
      ledc_stop(LEDC_LOW_SPEED_MODE, LEDC_CHANNEL_0, 0);  // ch0 on GPIO1 in your setup【flicker.txt:contentReference[oaicite:4]{index=4}】
      gpio_hold_dis(GPIO_NUM_1);
      gpio_set_direction(GPIO_NUM_1, GPIO_MODE_OUTPUT);
      gpio_set_level(GPIO_NUM_1, 0);
      gpio_hold_en(GPIO_NUM_1);
      quiet_hold_active_ = true;
    }
  }
  
  /** End peek LED quiet */
  void end_peek_led_quiet_() {
    if (!quiet_hold_active_) return;
    gpio_hold_dis(GPIO_NUM_1);
    // Give LEDC ch0 the pin back; duty will be whatever HP (light) last set (0 when off)
    ledc_update_duty(LEDC_LOW_SPEED_MODE, LEDC_CHANNEL_0);  // safe no-op if duty=0
    quiet_hold_active_ = false;
  }
  
  /** Set TX power dBm */
  void set_tx_power_dbm_(float dbm){
    // esp32 uses quarter-dBm units (0..84 typically). Clamp for safety.
    int q = (int)std::lround(dbm * 4.0f); if(q < 8) q = 8; if(q > 84) q = 84;
    esp_wifi_set_max_tx_power((int8_t)q);
  }
  
  /** Find dedup slot */
  int dedup_find_slot_(const uint8_t* mac, uint8_t type) {
    int free_idx = -1;
    int lru_idx = 0;
    uint32_t lru_time = 0xFFFFFFFFu;
    for (int i = 0; i < kDedupSlots; ++i) {
      if (dedup_[i].last_seen_ms == 0 && free_idx < 0) free_idx = i;
      if (std::memcmp(dedup_[i].mac, mac, 6) == 0 && dedup_[i].type == type) return i;
      if (dedup_[i].last_seen_ms < lru_time) { lru_time = dedup_[i].last_seen_ms; lru_idx = i; }
    }
    return (free_idx >= 0) ? free_idx : lru_idx;
  }
  
  /** Get preset name from ID */
  static inline const char* preset_name_from_id_(uint8_t id) {
    switch (id) {
      case 0: return "Calm Pillar";
      case 1: return "Standard Candle";
      case 2: return "Fireplace";
      case 3: return "Nightlight Glow";
      default: return nullptr;
    }
  }
  
  /** Send command burst */
  void send_cmd_burst_(uint8_t cmd_id, uint8_t arg, uint16_t ttl_s) {
    CmdPkt p{PKT_CMD, group_, cmd_id, arg, ttl_s, ++seq_, current_hive_time(), 0};
    p.crc = compute_crc16_((uint8_t*)&p, sizeof(p) - sizeof(p.crc));

    float avg_rssi = calc_avg_rssi_();
    int frames = queen_mode_
                 ? (avg_rssi > -50 ? 7 : 15)
                 : (avg_rssi > -50 ? kWakeBurstFramesBase : 12);

    send_burst_(BCAST, (uint8_t*)&p, sizeof(p), frames, kWakeBurstDelayMs);
  }

  /** Check if dedup is dup and update */
  bool dedup_is_dup_and_update_(const uint8_t* mac, uint8_t type, uint32_t seq) {
    if (!mac) return false;
    int idx = dedup_find_slot_(mac, type);
    // If this slot belongs to a different (mac,type), rebind it.
    if (std::memcmp(dedup_[idx].mac, mac, 6) != 0 || dedup_[idx].type != type) {
      std::memcpy(dedup_[idx].mac, mac, 6);
      dedup_[idx].type = type;
      dedup_[idx].last_seq = 0;
    }
    bool dup = (seq <= dedup_[idx].last_seq);
    if (seq > dedup_[idx].last_seq) dedup_[idx].last_seq = seq;
    dedup_[idx].last_seen_ms = now_ms();
    return dup;
  }  
  
  /** Check if bad MAC */
  bool is_bad_mac_(const uint8_t mac[6]) const {
    static const uint8_t Z[6] = {0,0,0,0,0,0};
    static const uint8_t B[6] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};
    return (mac_compare(mac, Z) == 0) || (mac_compare(mac, B) == 0);
  }
  
  /** Init persistence for diagnostics */
  void diag_init_persistence_() {
    uint8_t ver_tmp{0};
    if (pref_prefs_version_.load(&ver_tmp)) {
      prefs_version_    = ver_tmp;
      prefs_version_ok_ = (ver_tmp == kPrefsVersion);
      std::strncpy(rc_persist_source_, prefs_version_ok_ ? "NVS_LOAD" : "NVS_MIGRATE", sizeof(rc_persist_source_) - 1);
    } else {
      uint8_t vcur = kPrefsVersion;
      pref_prefs_version_.save(&vcur);
      prefs_version_    = vcur;
      prefs_version_ok_ = true;
      std::strncpy(rc_persist_source_, "NVS_INIT", sizeof(rc_persist_source_) - 1);
    }

    uint32_t bc_tmp{0};
    if (!pref_boot_counter_.load(&bc_tmp)) bc_tmp = 0;
    boot_counter_ = bc_tmp + 1U;
    pref_boot_counter_.save(&boot_counter_);

    // Mark boot as the initial RC "change" for timeline provenance.
    last_rc_change_ms_ = now_ms();
    std::strncpy(last_rc_change_reason_, "BOOT_LOAD", sizeof(last_rc_change_reason_) - 1);
    rc_change_count_++;
  }
  
  /** Handle RC change */
  void on_rc_changed_(const char* reason){
    if (reason != nullptr) {
      std::strncpy(last_rc_change_reason_, reason, sizeof(last_rc_change_reason_) - 1);
      last_rc_change_reason_[sizeof(last_rc_change_reason_) - 1] = '\0';
    }
    last_rc_change_ms_ = now_ms();
    rc_change_count_++;
  }

  // ---------------- Private: Wi-Fi and Channel Helpers ----------------
  /** Set channel */
  void set_channel_(uint8_t ch){ esp_wifi_scan_stop(); if(ch>=1&&ch<=13) esp_wifi_set_channel(ch, WIFI_SECOND_CHAN_NONE); }

  /** Refresh home channel */
  void refresh_home_ch_(){ wifi_ap_record_t ap{}; if(esp_wifi_sta_get_ap_info(&ap)==ESP_OK){ if(ap.primary>=1&&ap.primary<=13) home_ch_=ap.primary; } }

  /** Check if connected */
  bool is_connected_() const { wifi_ap_record_t ap{}; return esp_wifi_sta_get_ap_info(&ap)==ESP_OK; }

  /** Ensure detached */
  void ensure_detached_(){ esp_wifi_scan_stop(); esp_wifi_disconnect(); uint32_t until=now_ms()+2000; while(is_connected_() && now_ms()<until) delay(10); }

  /** Bump used channel */
  void bump_used_(uint8_t ch){ if(ch>=1&&ch<=13) used_ch_mask_ |= (1u<<ch); }

  /** Count bits in 16-bit mask */
  int count_bits16_(uint16_t m){ int c=0; for(int i=0;i<16;i++) if(m&(1u<<i)) c++; return c; }

  /** Get first in mask */
  uint8_t first_in_mask_(uint16_t m){ for(int ch=1; ch<=13; ++ch) if(m&(1u<<ch)) return (uint8_t)ch; return 1; }

  /** Get next in mask */
  uint8_t next_in_mask_(uint16_t m, uint8_t ch){ for(int i=0;i<13;i++){ ch=(ch>=13)?1:(uint8_t)(ch+1); if(m&(1u<<ch)) return ch; } return ch; }

  /** Compute AP mask */
  uint16_t compute_ap_mask_(const uint8_t* ssid_filter){
    wifi_scan_config_t cfg{}; cfg.bssid=nullptr; cfg.channel=0; cfg.show_hidden=true; cfg.scan_type=WIFI_SCAN_TYPE_PASSIVE; cfg.scan_time.passive=120;
    cfg.ssid = (uint8_t*)ssid_filter; // nullptr => scan all; otherwise filter to our SSID
    esp_wifi_scan_start(&cfg, true);
    uint16_t n=0; esp_wifi_scan_get_ap_num(&n); if(n>32) n=32; wifi_ap_record_t aps[32]; esp_wifi_scan_get_ap_records(&n, aps);
    uint16_t mask=0; for(uint16_t i=0;i<n;i++){
      int ch=aps[i].primary; if(ch>=1&&ch<=13) mask |= (1u<<ch);
    }
    ESP_LOGD("espnow", "Computed AP mask: 0x%04X from %u APs", mask, n);
    return mask;
  }

  /** Choose RC from mask */
  uint8_t choose_rc_from_mask_(uint16_t mask){ 
    // Weight by avg RSSI per channel from peers we heard during link
    float ch_rssi_avg[14] = {0};
    int   ch_counts[14]   = {0};

    for(int i=0; i<peer_count_; i++) {
      uint8_t ch = peers_[i].home_ch;
      if (ch >= 1 && ch <= 13) {
        ch_rssi_avg[ch] = (ch_rssi_avg[ch] * ch_counts[ch] + peers_[i].rssi) / (ch_counts[ch] + 1);
        ch_counts[ch]++;
      }
    }

    // Pick best by histogram, lightly favoring stronger RSSI
    int best = -1;
    uint8_t best_ch = home_ch_;
    for (int ch = 1; ch <= 13; ++ch) {
      if (mask & (1u << ch)) {
        float weight = (ch_rssi_avg[ch] > -60 ? 1.5f :
                        (ch_rssi_avg[ch] < -80 ? 0.5f : 1.0f));
        int weighted_hist = (int)(resp_hist_[ch] * weight);
        if (weighted_hist > best) {
          best = weighted_hist;
          best_ch = (uint8_t) ch;
        }
      }
    }

    if (best > 0) return best_ch;
    return (home_ch_ >= 1 && home_ch_ <= 13) ? home_ch_ : 6;
  }

  // ---------------- Private: CRC and Sending Helpers ----------------
  /** Compute CRC16 */
  uint16_t compute_crc16_(const uint8_t* data, size_t len) {
    uint16_t crc = 0xFFFF;
    for (size_t i = 0; i < len; ++i) {
      crc ^= (uint16_t)data[i] << 8;
      for (uint8_t j = 0; j < 8; ++j) {
        crc = (crc & 0x8000) ? (crc << 1) ^ 0x1021 : crc << 1;
      }
    }
    return crc;
  }

  /** Send wake burst */
  void send_wake_burst_(uint16_t ttl_s){
    WakePkt p{PKT_WAKE, group_, ttl_s, ++seq_, current_hive_time(), 0};
    p.crc = compute_crc16_((uint8_t*)&p, sizeof(p) - sizeof(p.crc));
    // Dynamic frames
    float avg_rssi = calc_avg_rssi_();
    int frames = queen_mode_ ? (avg_rssi > -50 ? 7 : 15) : (avg_rssi > -50 ? kWakeBurstFramesBase : 12);
    send_burst_(BCAST, (uint8_t*)&p, sizeof(p), frames, kWakeBurstDelayMs);
  }

  /** Calculate average RSSI */
  float calc_avg_rssi_() {
    if (peer_count_ == 0) return last_rx_rssi_;
    float sum = 0; for(int i=0; i<peer_count_; i++) sum += peers_[i].rssi;
    return sum / peer_count_;
  }

  /** Temp boost send */
  void temp_boost_send(const uint8_t* mac, const uint8_t* data, int len) {
    bool need_boost = link_phase_ == LinkPhase::IDLE && !is_waking_;
    if (!need_boost) {
      esp_err_t err = esp_now_send(mac, data, len);
      if (err != ESP_OK) {
        ESP_LOGW("espnow", "esp_now_send err=%d to %02X:%02X:%02X:%02X:%02X:%02X (no-boost)",
                 (int)err, mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
      }
      return;
    }
    wifi_ps_type_t old_ps;
    esp_wifi_get_ps(&old_ps);
    int8_t old_tx_q;
    esp_wifi_get_max_tx_power(&old_tx_q);
    float old_tx = old_tx_q / 4.0f;

    esp_wifi_set_ps(WIFI_PS_NONE);
    set_tx_power_dbm_(kTxBoostDbm);
    esp_err_t err = esp_now_send(mac, data, len);
    if (err != ESP_OK) {
      ESP_LOGW("espnow", "esp_now_send err=%d to %02X:%02X:%02X:%02X:%02X:%02X (boost)",
               (int)err, mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
    }
    esp_wifi_set_ps(old_ps);
    set_tx_power_dbm_(old_tx);
  }

  /** Send burst */
  void send_burst_(const uint8_t* addr, const uint8_t* data, size_t len, int frames, uint32_t delay_ms) {
    for(int i=0; i<frames; i++) {
      esp_now_send(addr, data, len);
      delay(delay_ms);
    }
  }

  /** Relay if edge */
  void relay_if_edge_(const uint8_t* data, size_t len, uint16_t ttl_s, size_t ttl_offset, size_t hive_time_offset, int rssi_threshold = -70) {
    if (ttl_s <= 1 || last_rx_rssi_ >= rssi_threshold) return;
    std::vector<uint8_t> relay_data(data, data + len);
    uint16_t new_ttl = ttl_s - 1;
    relay_data[ttl_offset] = static_cast<uint8_t>(new_ttl & 0xFF);
    relay_data[ttl_offset + 1] = static_cast<uint8_t>(new_ttl >> 8);
    uint32_t new_hive_time = current_hive_time();
    relay_data[hive_time_offset] = static_cast<uint8_t>(new_hive_time & 0xFF);
    relay_data[hive_time_offset + 1] = static_cast<uint8_t>((new_hive_time >> 8) & 0xFF);
    relay_data[hive_time_offset + 2] = static_cast<uint8_t>((new_hive_time >> 16) & 0xFF);
    relay_data[hive_time_offset + 3] = static_cast<uint8_t>(new_hive_time >> 24);
    uint16_t new_crc = compute_crc16_(relay_data.data(), len - sizeof(uint16_t));
    relay_data[len - 2] = static_cast<uint8_t>(new_crc & 0xFF);
    relay_data[len - 1] = static_cast<uint8_t>(new_crc >> 8);
    uint32_t jitter = (seq_ % 51) + 50; // 50-100ms jitter
    delay(jitter);
    esp_now_send(BCAST, relay_data.data(), len);
  }

  /** Sync queen time */
  void sync_queen_time_() {
    auto time = id(my_time).now();
    if(time.is_valid()) {
      uint32_t seconds_since_midnight = time.hour * 3600 + time.minute * 60 + time.second;
      uint32_t ms_since_midnight = seconds_since_midnight * 1000UL + (millis() % 1000);
      time_offset_ = (int64_t)ms_since_midnight - (int64_t)now_ms();
      last_sync_ms_ = now_ms();
    }
  }

  // ---------------- Private: Peer Management Helpers ----------------
  /** Find peer */
  int find_peer_(const uint8_t* mac){ for(int i=0;i<peer_count_; ++i){ if(std::memcmp(peers_[i].mac, mac, 6)==0) return i; } return -1; }

  /** Ensure peer */
  int ensure_peer_(const uint8_t* mac, uint8_t ch){ int idx=find_peer_(mac); if(idx>=0){ peers_[idx].home_ch=ch; return idx; } if(peer_count_>=kMaxPeers) return -1; idx=peer_count_++; std::memcpy(peers_[idx].mac, mac, 6); peers_[idx].home_ch=ch; peers_[idx].acked=false; peers_[idx].tries=0; peers_[idx].added=false; return idx; }

  /** Add peer entry */
  void add_peer_entry_(int idx){ if(idx<0) return; if(peers_[idx].added) return; esp_now_peer_info_t pi{}; std::memcpy(pi.peer_addr, peers_[idx].mac, 6); pi.ifidx=WIFI_IF_STA; pi.channel=0; pi.encrypt=false; if(esp_now_add_peer(&pi)==ESP_OK) peers_[idx].added=true; }

  /** Clear peers */
  void clear_peers_(){ for(int i=0;i<peer_count_; ++i){ if(peers_[i].added) esp_now_del_peer(peers_[i].mac); } peer_count_=0; }

  // ---------------- Private: Link Set Helpers ----------------
  /** Broadcast link set full */
  void broadcast_link_set_full_(uint8_t rc, uint16_t mask){
    LinkSet set{}; set.type=PKT_LINK_SET; set.group=group_; set.rc_ch=rc; set.used_mask=mask; std::memcpy(set.leader_mac,self_mac_,6); std::memcpy(set.queen_mac, queen_mac_, 6); set.ttl_s = 1; set.seq=++seq_; set.hive_time_ms = current_hive_time(); 
    set.crc = compute_crc16_((uint8_t*)&set, sizeof(set) - sizeof(set.crc));
    for(uint8_t ch=1; ch<=13; ++ch){ if(!(mask&(1u<<ch))) continue; set_channel_(ch); send_burst_(BCAST, (uint8_t*)&set, sizeof(set), 5, 10); uint32_t until=now_ms()+200; while(now_ms()<until) delay(1);} 
    for(uint8_t ch2: {rc, home_ch_}){ set_channel_(ch2); send_burst_(BCAST, (uint8_t*)&set, sizeof(set), 5, 10); uint32_t until2=now_ms()+200; while(now_ms()<until2) delay(1);} 
    tx_set_++;
  }

  /** Unicast sets with ack */
  void unicast_sets_with_ack_(uint8_t rc){
    LinkSet set{}; set.type=PKT_LINK_SET; set.group=group_; set.rc_ch=rc; set.used_mask=used_ch_mask_;
    std::memcpy(set.leader_mac, self_mac_, 6); std::memcpy(set.queen_mac, queen_mac_, 6); set.ttl_s = 1; set.seq=++seq_; set.hive_time_ms = current_hive_time(); 
    set.crc = compute_crc16_((uint8_t*)&set, sizeof(set) - sizeof(set.crc));

    for(int i=0;i<peer_count_; ++i){ peers_[i].acked=false; peers_[i].tries=0; add_peer_entry_(i); }

    int maxTries = 8;
    uint32_t wait_ms = 800U; // Increased to 800U for better ACK chance

    for(int i=0;i<peer_count_; ++i){
      uint32_t deadline = now_ms() + (uint32_t)maxTries * (wait_ms + 100U);
      while(!peers_[i].acked && peers_[i].tries<maxTries && now_ms()<deadline){
        set_channel_(peers_[i].home_ch);
        send_burst_(peers_[i].mac, (uint8_t*)&set, sizeof(set), 3, 50);
        peers_[i].tries++;
        uint32_t w=now_ms()+wait_ms; while(now_ms()<w && !peers_[i].acked) delay(5);
      }
      if (!peers_[i].acked) {
        ESP_LOGW("espnow", "No ACK from peer %02X:%02X:%02X:%02X:%02X:%02X after %d tries - re-broadcasting SET", peers_[i].mac[0], peers_[i].mac[1], peers_[i].mac[2], peers_[i].mac[3], peers_[i].mac[4], peers_[i].mac[5], maxTries);
        set_channel_(peers_[i].home_ch);
        send_burst_(BCAST, (uint8_t*)&set, sizeof(set), 3, 50);
      }
    }
  }

  /** Finish link */
  void finish_link_(uint8_t final_rc){
    rc_ch_ = final_rc;
    on_rc_changed_("LINK_COMPLETE");   // [ADD] provenance

    used_ch_mask_ = (uint16_t)(ap_mask_ | (uint16_t)(1u<<home_ch_) | (uint16_t)(1u<<rc_ch_));
    if (rc_ch_>=1 && rc_ch_<=13) pref_rc_.save(&rc_ch_);
    pref_mask_.save(&used_ch_mask_);

    set_channel_(home_ch_);
    wifi_detached_ = false;
    link_phase_    = LinkPhase::IDLE;
    link_curr_ch_  = 0;
    esp_wifi_connect();

    deferred_peer_clear_ms_ = now_ms() + 30000U;

    esp_wifi_set_ps(WIFI_PS_MIN_MODEM);
    set_tx_power_dbm_(kTxBaseDbm);

    // Re-lock RC and clear arming window
    rc_locked_ = true;
    { uint8_t v = 1; pref_rc_lock_.save(&v); }
    accept_set_until_ms_ = 0;
    last_link_complete_ms_ = now_ms();

    if (mac_compare(queen_mac_, BCAST) == 0) {
      ESP_LOGW("espnow", "Queen MAC not set - re-run link on central device");
    }
    ESP_LOGI("espnow","Link complete: RC=%u used=0x%04X (RC locked)", rc_ch_, used_ch_mask_);
  }

  // ---------------- Private: Loop Helpers for Peek, Wake, Link ----------------
  /** Loop peek */
  void loop_peek_() {
    uint32_t t=now_ms(); if (t>=next_forced_detach_peek_ms_) { esp_wifi_scan_stop(); esp_wifi_disconnect(); if(peek_ch_>=1&&peek_ch_<=13) set_channel_(peek_ch_); next_forced_detach_peek_ms_=t+kForcedDetachIntervalMs; }
    if (now_ms()>=peek_end_ms_) { is_peeking_=false; peek_ch_=0; end_peek_led_quiet_(); if(!is_waking_ && link_phase_ == LinkPhase::IDLE) { esp_wifi_set_ps(WIFI_PS_MIN_MODEM); } }
  }

  /** Loop wake */
  void loop_wake_() {
    uint32_t t = now_ms();

    // Maintain periodic forced-detach (unchanged)
    if (t >= next_forced_detach_wake_ms_) { 
      esp_wifi_scan_stop(); esp_wifi_disconnect(); 
      set_channel_(wake_ch_); 
      next_forced_detach_wake_ms_ = t + kForcedDetachIntervalMs; 
    }

    // End of beacon window: restore WiFi/PS (unchanged)
    if (t >= wake_end_ms_) {
      is_waking_ = false; 
      refresh_home_ch_(); 
      set_channel_(home_ch_);
      // restore power save and base TX power after beaconing
      esp_wifi_set_ps(WIFI_PS_MIN_MODEM); 
      set_tx_power_dbm_(kTxBaseDbm);
      wifi_set_auto_connect(true); 
      esp_wifi_connect(); 
      wifi_detached_ = false; 
      return;
    }

    // --- NEW: starter triple-tap when Wake-Now starts ---
    if (wake_entry_bursts_remaining_ > 0) {
      if (wake_entry_burst_last_ms_ == 0 || (t - wake_entry_burst_last_ms_) >= kEntryTripleTapSpacingMs) {
        send_wake_burst_(wake_ttl_s_);
        tx_wake_++;
        wake_entry_burst_last_ms_ = t;
        wake_entry_bursts_remaining_--;
      }
      return; // finish triple-tap before regular cadence (existing behavior below)
    }

    // Existing behavior: emit micro-bursts (as currently implemented)
    send_wake_burst_(wake_ttl_s_);
    tx_wake_++;
  }

  /** Loop link */
  void loop_link_() {
    uint32_t tn = now_ms();
    if (tn >= next_forced_detach_ms_) {
      esp_wifi_scan_stop();
      esp_wifi_disconnect();
      next_forced_detach_ms_ = tn + kForcedDetachIntervalMs;
    }
    uint32_t t = now_ms();

    if (t - last_log_ms_ >= 1000) {
      ESP_LOGD("espnow",
               "Link %s ch=%u hops=%u t+%us",
               link_phase_str(), link_ch_, link_hop_count_,
               (unsigned)((t - link_start_ms_) / 1000U));
      last_log_ms_ = t;
    }

    // Transition from WAKE_SWEEP -> PROBE when sweep time ends
    if (link_phase_ == LinkPhase::WAKE_SWEEP && t >= wake_sweep_end_ms_) {
      link_phase_       = LinkPhase::PROBE;
      link_last_hop_ms_ = t;
      link_ch_          = first_in_mask_(ap_mask_);
      set_channel_(link_ch_);
      link_curr_ch_     = link_ch_;
      link_hop_count_++;

      // --- NEW: prime triple-tap on entering PROBE first dwell? (not needed) ---
      // We only triple-tap during WAKE_SWEEP, so leave entry_bursts_* untouched here.
    }

    // End conditions for PROBE: broadcast SETs and finish link
    if (link_phase_ == LinkPhase::PROBE && t >= link_end_ms_) {
      bump_used_(home_ch_);
      uint8_t rc = choose_rc_from_mask_(used_ch_mask_);
      uint16_t set_mask = (uint16_t)(used_ch_mask_ | ap_mask_ |
                          (uint16_t)(1u << home_ch_) | (uint16_t)(1u << rc));
      for (int rep = 0; rep < 3; rep++) broadcast_link_set_full_(rc, set_mask); // Redundancy
      unicast_sets_with_ack_(rc);
      finish_link_(rc);
      return;
    }

    // Guard timeout (fallback) — compute and send SETs with best known RC/mask
    if (t >= guard_end_ms_) {
      bump_used_(home_ch_);
      uint8_t rc = choose_rc_from_mask_(used_ch_mask_);
      uint16_t set_mask = (uint16_t)(used_ch_mask_ | ap_mask_ |
                          (uint16_t)(1u << home_ch_) | (uint16_t)(1u << rc));
      for (int rep = 0; rep < 3; rep++) broadcast_link_set_full_(rc, set_mask); // Redundancy
      finish_link_(rc);
      return;
    }

    // Per-dwell timing
    uint32_t dwell     = (link_phase_ == LinkPhase::PROBE) ? (uint32_t)probe_dwell_ms_ : wake_per_ch_ms_;
    uint32_t dwell_end = link_last_hop_ms_ + dwell;

    if (link_phase_ == LinkPhase::WAKE_SWEEP) {
      // --- NEW: starter triple-tap on dwell entry (0/45/90 ms after entry) ---
      if (entry_bursts_remaining_ > 0) {
        if (entry_burst_last_ms_ == 0 ||
            (t - entry_burst_last_ms_) >= kEntryTripleTapSpacingMs) {
          send_wake_burst_(wake_ttl_s_);
          tx_wake_++;
          entry_burst_last_ms_ = t;
          entry_bursts_remaining_--;
        }
        // Hold off regular cadence until triple-tap completes
        return;
      }

      // Existing micro-burst cadence during WAKE sweep
      if (t - link_last_burst_ms_ >= kWakeBurstPeriodMs) {
        send_wake_burst_(wake_ttl_s_);
        tx_wake_++;
        link_last_burst_ms_ = t;
      }
    } else if (link_phase_ == LinkPhase::PROBE) {
      // Probe pacing with small randomized slice to avoid collisions
      uint32_t base = (uint32_t)std::max<uint16_t>((uint16_t)(probe_dwell_ms_ / 3), (uint16_t)30);
      int16_t jitter = (int16_t)((seq_ * 13U) % 21U) - 10; // -10..+10
      uint32_t slice = (uint32_t)std::max<int>(20, (int)base + (int)jitter);
      if ((t - last_probe_send_ms_) >= slice) {
        LinkProbe p{PKT_LINK_PROBE, group_, 1, ++seq_, current_hive_time(), 0};
        p.crc = compute_crc16_((uint8_t*)&p, sizeof(p) - sizeof(p.crc));
        esp_now_send(BCAST, (uint8_t*)&p, sizeof(p));
        last_probe_send_ms_ = t;
        probe_sent_count_this_dwell_++;
      }
    }

    // Hop to next channel when dwell is over
    if (t >= dwell_end) {
      link_ch_ = next_in_mask_(ap_mask_, link_ch_);
      set_channel_(link_ch_);
      link_curr_ch_ = link_ch_;
      link_hop_count_++;
      link_last_hop_ms_   = t;
      link_last_burst_ms_ = t - kWakeBurstPeriodMs;
      last_probe_send_ms_ = t;
      probe_sent_count_this_dwell_ = 0;
      resp_sent_this_phase_ = 0;  // Reset per dwell

      // --- NEW: prime triple-tap for the new dwell ---
      entry_bursts_remaining_ = 3;
      entry_burst_last_ms_    = 0;
    }
  }

  // ---------------- Private: Packet Extraction and Handling Helpers ----------------
  /** Extract packet */
  template<typename Pkt>
  const Pkt* extract_pkt_(const uint8_t* data, int len) {
    if (len != sizeof(Pkt)) return nullptr;
    return (const Pkt*)data;
  }

  /** Handle command */
  void handle_cmd_(const uint8_t* data, int len, RecvCbInfo info) {
    // Validate + cast
    const CmdPkt* p = extract_pkt_<CmdPkt>(data, len);
    if (!p) return;

    // Group gate (same semantics as other packets)
    if ((p->group & 0x0F) != (group_ & 0x0F)) return;

    // Ignore self-originated frames
    const uint8_t* src = rx_mac_from_info(info);
    if (src && mac_compare(src, self_mac_) == 0) return;

  // ------------------------------------------------------------------
  // NOTE: No TTL extension here (per your request).
  // We are intentionally NOT modifying hive_awake_until_.
  // ------------------------------------------------------------------

  // Edge relay parity with WAKE: decrement TTL, refresh hive_time_ms, recompute CRC, forward if at edge
  // (If you also want to drop relaying for CMD, just remove this block.)
    relay_if_edge_(data, len,
                 /*ttl_current=*/p->ttl_s,
                 /*ttl_field_off=*/offsetof(CmdPkt, ttl_s),
                 /*hivetime_field_off=*/offsetof(CmdPkt, hive_time_ms));

    // Command dispatch (one command per packet)
    switch (p->cmd_id) {
      case CMD_SIGNAL: {
          // Null Placeholder
        break;
      }
      case CMD_WAKE: { 
        handle_cmd_wake_((uint32_t)p->ttl_s * 1000UL);
        break;
      }
      case CMD_SET_BRIGHTNESS: { 
        const uint8_t lvl = p->arg;        // arg byte from your command packet
        set_saved_brightness_from_level(lvl, 140);
        set_ui_brightness_from_level(lvl, /*scale=*/140, /*transition_ms=*/0);  // If your fpsm_output maps s*140.0f -> level, use 140 here.
        fpsm_set_brightness(lvl);          // keep the hardware in sync (existing call)
        ESP_LOGI("espnow", "CMD_SET_BRIGHTNESS arg0=%u", (unsigned)p->arg);
        break;
      }
      case CMD_FPSM_ONOFF: {
        // 0 = off, 1 = on
        bool on = (p->arg != 0);
        fpsm_set_enabled(on);
        if (on) {
          auto call = ui_light_->turn_on();
          call.set_transition_length(0);
          call.perform(); } 
        else {
          auto call = ui_light_->turn_off();
          call.set_transition_length(0);
          call.perform();
        }
        ESP_LOGI("espnow", "CMD_FPSM_ONOFF arg=%u", (unsigned) p->arg);
        break;  
      }

      case CMD_SET_PRESET: {
        // Map arg (0..3) to your preset functions
        switch (p->arg) {
          case 0: fpsm_apply_preset_calm_pillar_v3();        break;
          case 1: fpsm_apply_preset_standard_candle_v3();    break;
          case 2: fpsm_apply_preset_fireplace_v3();          break;
          case 3: fpsm_apply_preset_nightlight_glow_v3();    break;
          default: /* unknown preset id -> ignore */          break;
        }
        set_preset_ui(p->arg);
        ESP_LOGI("espnow", "CMD_SET_PRESET arg=%u", (unsigned) p->arg);
        break;  // ADD HERE
      }
      default:
        break;
    }

    // Diagnostics breadcrumbs (reuse existing fields; optional to split out cmd-specific counters)
    const uint32_t now = now_ms();
    if (src) std::memcpy(last_wake_src_mac_, src, 6);  // reuse until you add last_cmd_* fields
    last_wake_seq_     = p->seq;                       // reuse until you add last_cmd_seq_
    last_wake_time_ms_ = now;                          // reuse until you add last_cmd_time_ms_
    rx_wake_++;                                         // or introduce rx_cmd_ if you prefer
  }

  /** Handle wake */
  void handle_wake_(const uint8_t* data, int len, RecvCbInfo info) {
    auto* p = extract_pkt_<WakePkt>(data, len);
    if (!p || (p->group & 0x0F) != (group_ & 0x0F)) return;

    // --- NEW: ignore self-originated WAKE (safety) ---
    const uint8_t* src = rx_mac_from_info(info);
    if (src && mac_compare(src, self_mac_) == 0) return;

    // --- NEW: TTL extension guard ---
    uint32_t now   = now_ms();
    uint32_t remain_ms = hive_awake_ms_remaining();
    uint32_t new_ms    = (uint32_t)p->ttl_s * 1000UL;

    // Only set/extend if incoming TTL is meaningfully larger than remaining (by > guard)
    if (remain_ms == 0 || new_ms > (remain_ms + kTtlExtendGuardMs)) {
      hive_awake_until_ = now + new_ms;
    }

    uint8_t ch = rx_channel_from_info(info);
    if (ch) bump_used_(ch);
    rx_wake_++;

    // --- NEW: breadcrumbs for triage ---
    if (src) std::memcpy(last_wake_src_mac_, src, 6);
    last_wake_seq_      = p->seq;
    last_wake_time_ms_  = now;

    // Throttled WAKE-phase LINK_RESP (existing behavior)
    uint32_t tnow = now_ms();
    if (tnow - last_resp_send_ms_ > kRespThrottleMs && resp_sent_this_phase_ < kRespCapPerPhase) {
      refresh_home_ch_();
      LinkResp r{PKT_LINK_RESP, group_, home_ch_, last_rx_rssi_, ++seq_, current_hive_time(), 0};
      r.crc = compute_crc16_((uint8_t*)&r, sizeof(r) - sizeof(r.crc));
      temp_boost_send(BCAST, (uint8_t*)&r, sizeof(r));
      tx_resp_++;
      last_resp_send_ms_ = tnow;
      resp_sent_this_phase_++;
    }

    // Relay if edge (existing behavior)
    relay_if_edge_(data, len, p->ttl_s, 2, 8);
  }
  
  /** Handle command wake */
  void handle_cmd_wake_(uint32_t new_ms) {

    // --- TTL extension guard ---
    uint32_t now   = now_ms();
    uint32_t remain_ms = hive_awake_ms_remaining();

    // Only set/extend if incoming TTL is meaningfully larger than remaining (by > guard)
    if (remain_ms == 0 || new_ms > (remain_ms + kTtlExtendGuardMs)) {
      hive_awake_until_ = now + new_ms;
    }

    // Throttled WAKE-phase LINK_RESP (existing behavior)
    uint32_t tnow = now_ms();
    if (tnow - last_resp_send_ms_ > kRespThrottleMs && resp_sent_this_phase_ < kRespCapPerPhase) {
      refresh_home_ch_();
      LinkResp r{PKT_LINK_RESP, group_, home_ch_, last_rx_rssi_, ++seq_, current_hive_time(), 0};
      r.crc = compute_crc16_((uint8_t*)&r, sizeof(r) - sizeof(r.crc));
      temp_boost_send(BCAST, (uint8_t*)&r, sizeof(r));
      tx_resp_++;
      last_resp_send_ms_ = tnow;
      resp_sent_this_phase_++;
    }

  }

  /** Handle link probe */
  void handle_link_probe_(const uint8_t* data, int len) {
    auto* p = extract_pkt_<LinkProbe>(data, len);
    if (!p || (p->group & 0x0F) != (group_ & 0x0F)) return;
    refresh_home_ch_();
    if (resp_sent_this_phase_ < kRespCapPerPhase) {
      LinkResp r{PKT_LINK_RESP, group_, home_ch_, last_rx_rssi_, ++seq_, current_hive_time(), 0};
      r.crc = compute_crc16_((uint8_t*)&r, sizeof(r) - sizeof(r.crc));
      temp_boost_send(BCAST, (uint8_t*)&r, sizeof(r));
      tx_resp_++;
      resp_sent_this_phase_++;
    }
    rx_probe_++;
    // Relay if edge
    relay_if_edge_(data, len, p->ttl_s, 2, 8);
    accept_set_until_ms_ = now_ms() + 5000U; // allow LINK_SET for 5s after a probe
  }

  /** Handle link resp */
  void handle_link_resp_(const uint8_t* data, int len, const uint8_t* mac) {
    auto* r = extract_pkt_<LinkResp>(data, len);
    if (!r || (r->group & 0x0F) != (group_ & 0x0F)) return;
    int idx = mac ? ensure_peer_(mac, r->home_ch) : -1;
    if (idx >=0) peers_[idx].rssi = r->rssi;
    bump_used_(r->home_ch);
    if (r->home_ch >= 1 && r->home_ch <= 13) resp_hist_[r->home_ch]++;
    rx_resp_++;
  }

  /** Handle link set */
  void handle_link_set_(const uint8_t* data, int len, const uint8_t* mac) {
    auto* s = extract_pkt_<LinkSet>(data, len);
    if (!s || (s->group & 0x0F) != (group_ & 0x0F)) return;

    // Existing arming logic (unchanged): only accept when "armed".
    bool armed = (link_phase_ != LinkPhase::IDLE) || (now_ms() <= accept_set_until_ms_) || (!rc_locked_);
    if (!armed) {
      ESP_LOGW("espnow", "Ignoring LINK_SET while RC locked (not armed)");
      return;
    }

    // Apply new RC/mask.
    rc_ch_ = s->rc_ch;
    on_rc_changed_("LINK_SET");
    used_ch_mask_ = s->used_mask;

    // [NEW] Persist immediately so a reboot cannot resurrect an old RC.
    if (rc_ch_ >= 1 && rc_ch_ <= 13) {
      pref_rc_.save(&rc_ch_);
    }
    pref_mask_.save(&used_ch_mask_);
    ESP_LOGI("espnow", "Persisted RC=%u mask=0x%04X from LINK_SET", rc_ch_, (unsigned)used_ch_mask_);

    // Harden queen MAC update (ignore zero/broadcast).
    if (!is_bad_mac_(s->queen_mac)) {
      std::memcpy(queen_mac_, s->queen_mac, 6);
      pref_queen_mac_.save(queen_mac_);
    } else {
      ESP_LOGW("espnow","Ignoring invalid queen MAC in LINK_SET");
    }

    // Maintain peer / ACK behavior.
    int idx = -1;
    if (mac) {
      idx = ensure_peer_(mac, s->rc_ch);
      if (idx >= 0) add_peer_entry_(idx);
    }

    bump_used_(s->rc_ch);
    rx_set_++;

    // ACK unicast.
    if (mac) {
      LinkAck a{PKT_LINK_ACK, group_, rc_ch_, {0}, s->seq, current_hive_time(), 0};
      std::memcpy(a.mac, self_mac_, 6);
      a.crc = compute_crc16_((uint8_t*)&a, sizeof(a) - sizeof(a.crc));
      temp_boost_send(mac, (uint8_t*)&a, sizeof(a));
      tx_ack_++;
    }

    // Relay if edge.
    relay_if_edge_(data, len, s->ttl_s, 17, 23);
  }

  /** Handle link ack */
  void handle_link_ack_(const uint8_t* data, int len, const uint8_t* mac) {
    auto* a = extract_pkt_<LinkAck>(data, len);
    if (!a || (a->group & 0x0F) != (group_ & 0x0F)) return;
    int idx = mac ? find_peer_(mac) : -1;
    if (idx >= 0) { peers_[idx].acked = true; ESP_LOGI("espnow", "Received ACK from %02X:%02X:%02X:%02X:%02X:%02X seq=%u", mac[0], mac[1], mac[2], mac[3], mac[4], mac[5], a->seq); }
    rx_ack_++;
  }

  // ---------------- Private: Callback Functions ----------------
  /** Receive callback */
  ESPNOW_RECV_SIG(info, data, len) {
    if (!self_ || len < 1) return;
    self_->last_rx_type_ = data[0];
    self_->last_rx_rssi_ = rx_rssi_from_info(info);
    const uint8_t* mac = rx_mac_from_info(info);
    if (mac) std::memcpy(self_->last_rx_mac_, mac, 6);

    // CRC check
    uint16_t expected_crc = *((uint16_t*)(data + len - sizeof(uint16_t)));
    uint16_t computed_crc = self_->compute_crc16_(data, len - sizeof(uint16_t));
    if (expected_crc != computed_crc) {
      ESP_LOGW("espnow", "CRC mismatch; dropping packet type=%u", data[0]);
      return;
    }

    // Deduplication check using seq
    uint32_t pkt_seq = 0;
    switch (data[0]) {
      case PKT_WAKE:      pkt_seq = ((WakePkt*)data)->seq; break;
      case PKT_LINK_PROBE:pkt_seq = ((LinkProbe*)data)->seq; break;
      case PKT_LINK_RESP: pkt_seq = ((LinkResp*)data)->seq; break;
      case PKT_LINK_SET:  pkt_seq = ((LinkSet*)data)->seq; break;
      case PKT_LINK_ACK:  pkt_seq = ((LinkAck*)data)->seq; break;
      case PKT_CMD:       pkt_seq = ((CmdPkt*)data)->seq; break;
      
      default: break;
    }

    if (mac && self_->dedup_is_dup_and_update_(mac, data[0], pkt_seq)) {
      ESP_LOGD("espnow", "Duplicate seq %u for type=%u from %02X:%02X:%02X:%02X:%02X:%02X; dropping",
               pkt_seq, data[0], mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
      return;
    }

    ESP_LOGD("espnow", "Received type=%u from %02X:%02X:%02X:%02X:%02X:%02X RSSI=%d",
             data[0], mac[0], mac[1], mac[2], mac[3], mac[4], mac[5], self_->last_rx_rssi_);

    uint32_t hive_time_ms = 0;
    switch (data[0]) {
      case PKT_WAKE: hive_time_ms = ((WakePkt*)data)->hive_time_ms; break;
      case PKT_LINK_PROBE: hive_time_ms = ((LinkProbe*)data)->hive_time_ms; break;
      case PKT_LINK_RESP: hive_time_ms = ((LinkResp*)data)->hive_time_ms; break;
      case PKT_LINK_SET: hive_time_ms = ((LinkSet*)data)->hive_time_ms; break;
      case PKT_LINK_ACK: hive_time_ms = ((LinkAck*)data)->hive_time_ms; break;
      case PKT_CMD:      hive_time_ms = ((CmdPkt*)data)->hive_time_ms; break; 
    }
    if(hive_time_ms > 0) {
      self_->time_offset_ = (int64_t)hive_time_ms - (int64_t)now_ms() - 20; // Approx prop delay
      self_->last_sync_ms_ = now_ms();
      self_->time_synced_ = true;
    }

    switch (data[0]) {
      case PKT_WAKE: self_->handle_wake_(data, len, info); break;
      case PKT_LINK_PROBE: self_->handle_link_probe_(data, len); break;
      case PKT_LINK_RESP: self_->handle_link_resp_(data, len, mac); break;
      case PKT_LINK_SET: self_->handle_link_set_(data, len, mac); break;
      case PKT_LINK_ACK: self_->handle_link_ack_(data, len, mac); break;
      case PKT_CMD:      self_->handle_cmd_(data, len, info); break;
    }
  }

  /** Send callback */
  static void send_cb_(const uint8_t *mac_addr, esp_now_send_status_t status) {
    if (!self_) return;
    const uint8_t* mac = (const uint8_t*)mac_addr;
    if (!mac) return;

    // Check if send failed (status != ESP_OK) and queue for retry if applicable
    if (status != ESP_OK) {
      ESP_LOGW("espnow", "Send failed to %02X:%02X:%02X:%02X:%02X:%02X", mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
    }
  }
};