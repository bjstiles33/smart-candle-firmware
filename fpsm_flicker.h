#pragma once
#include "esphome/core/log.h"
#include "esphome/core/hal.h"

bool fpsm_init();              // load LP blob + prepare symbols
bool fpsm_start();             // start LP core
void fpsm_stop();              // stop LP core
void fpsm_set_enabled(bool en);
void fpsm_set_brightness(uint8_t level);
bool fpsm_is_enabled();
bool fpsm_is_running();

void fpsm_set_env_cycles(uint8_t env_cycles);      // 20..200
void fpsm_set_frame_pause(uint32_t cycles);        // extra delay cycles between envelopes
void fpsm_set_shimmer_amp(uint8_t amp);            // 0..32
void fpsm_set_jitter_hold(uint8_t hold);           // 1..64
void fpsm_set_drift_div(uint8_t div);              // 1..6 (bigger=slower)
void fpsm_set_breath_smooth(uint8_t smooth);       // 8..64 (bigger=slower)
void fpsm_set_lvl_smooth(uint8_t smooth);          // 8..64 (higher = smoother, slower)
void fpsm_set_target_stride(uint8_t stride);       // 1..64 (lower = more responsive)

void fpsm_apply_preset_calm_pillar_v3();
void fpsm_apply_preset_standard_candle_v3();
void fpsm_apply_preset_fireplace_v3();
void fpsm_apply_preset_nightlight_glow_v3();