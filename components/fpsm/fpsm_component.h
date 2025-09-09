#pragma once
//#include "esphome/core/component.h"
#include "esphome/core/log.h"

// Minimal shim so ESPHome builds/links the fpsm component folder.
// No behavior here — your YAML still calls the C API (fpsm_* functions).
class FpsmComponent : public esphome::Component {
 public:
  void setup() override {
    ESP_LOGD("fpsm_component", "FpsmComponent setup()");
  }
  void loop() override {
    // No periodic work. All control is via fpsm_* functions from YAML.
  }
};
