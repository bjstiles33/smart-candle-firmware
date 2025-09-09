# __init__.py  (no cg.include; works on older ESPHome)
import esphome.codegen as cg
import esphome.config_validation as cv
from esphome.const import CONF_ID

DEPENDENCIES = []
AUTO_LOAD = []

FpsmComponent = cg.global_ns.class_("FpsmComponent", cg.Component)

CONFIG_SCHEMA = cv.Schema({cv.GenerateID(): cv.declare_id(FpsmComponent)})

async def to_code(config):
    var = cg.new_Pvariable(config[CONF_ID])
    await cg.register_component(var, config)
