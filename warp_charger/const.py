"""Plain data tables for the WARP API. No Home Assistant imports."""

from dataclasses import dataclass

CHARGER_STATE = {
    0: "not_connected",
    1: "waiting_for_release",
    2: "ready",
    3: "charging",
    4: "error",
}

IEC61851_STATE = {0: "a", 1: "b", 2: "c", 3: "d", 4: "e"}

ERROR_STATE = {
    0: "ok",
    2: "switch_error",
    3: "dc_fault",
    4: "contactor_error",
    5: "communication_error",
}

DC_FAULT_STATE = {
    0: "ok",
    1: "dc_6ma",
    2: "system",
    3: "unknown",
    4: "calibration",
    5: "ac",
    6: "ac_dc",
}

CHARGE_MODE = {
    0: "fast",
    1: "off",
    2: "pv",
    3: "min_pv",
    4: "default",
    5: "min",
    6: "eco",
    7: "eco_pv",
    8: "eco_min",
    9: "eco_min_pv",
}
CHARGE_MODE_DEFAULT = 4  # state only, never selectable

DURATION_LIMITS = {
    0: "off",
    1: "15m",
    2: "30m",
    3: "45m",
    4: "1h",
    5: "2h",
    6: "3h",
    7: "4h",
    8: "6h",
    9: "8h",
    10: "12h",
}

INT32_MAX = 2147483647


@dataclass(frozen=True)
class MeterValue:
    """One meter value id from the firmware's ``meter_value_id.csv``."""

    key: str
    unit: str | None
    digits: int


# Source: Tinkerforge/esp32-firmware software/src/modules/meters/meter_value_id.csv.
# key = snake_case of the CSV columns measurand, submeasurand, phase, direction joined
# by "_" (empty columns and the "*Any" wildcard skipped; "kind" is not part of the key),
# e.g. "Power | Active | L1 | Im Ex Diff" -> "power_active_l1_im_ex_diff".
# unit/digits are taken verbatim from the CSV.
METER_VALUES: dict[int, MeterValue] = {
    1: MeterValue("voltage_l1_n", "V", 1),
    2: MeterValue("voltage_l2_n", "V", 1),
    3: MeterValue("voltage_l3_n", "V", 1),
    10: MeterValue("voltage_dc", "V", 1),
    13: MeterValue("current_l1_im_ex_sum", "A", 3),
    14: MeterValue("current_l1_im_ex_diff", "A", 3),
    17: MeterValue("current_l2_im_ex_sum", "A", 3),
    18: MeterValue("current_l2_im_ex_diff", "A", 3),
    21: MeterValue("current_l3_im_ex_sum", "A", 3),
    22: MeterValue("current_l3_im_ex_diff", "A", 3),
    35: MeterValue("current_dc", "A", 3),
    39: MeterValue("power_active_l1_im_ex_diff", "W", 0),
    48: MeterValue("power_active_l2_im_ex_diff", "W", 0),
    57: MeterValue("power_active_l3_im_ex_diff", "W", 0),
    74: MeterValue("power_active_l_sum_im_ex_diff", "W", 0),
    160: MeterValue("power_dc", "W", 0),
    209: MeterValue("energy_active_l_sum_import", "kWh", 3),
    211: MeterValue("energy_active_l_sum_export", "kWh", 3),
    213: MeterValue("energy_active_l_sum_im_ex_sum", "kWh", 3),
    364: MeterValue("frequency_l_avg", "Hz", 3),
    387: MeterValue("state_of_charge", "%", 1),
    405: MeterValue("energy_dc_import", "kWh", 3),
    407: MeterValue("energy_dc_export", "kWh", 3),
    430: MeterValue("voltage_pv_avg", "V", 1),
    441: MeterValue("current_pv_sum_export", "A", 3),
    453: MeterValue("power_pv_sum_im_ex_diff", "W", 0),
    472: MeterValue("energy_pv_sum_export", "kWh", 3),
    475: MeterValue("capacity", "kWh", 3),
}
