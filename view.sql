select 
componentSerialNumber,
frm_gen_ts,
convertedEquipmentParameters.protocol,
convertedEquipmentParameters.networkId,
convertedEquipmentParameters.deviceId,
convertedEquipmentParameters.parameters.`190`, 
convertedEquipmentParameters.parameters.`174`, 
convertedEquipmentParameters.parameters.`175`
from
(
  select
  componentSerialNumber,
  frm_gen_ts,
  explode(samples.convertedEquipmentParameters) as convertedEquipmentParameters
  from
  (
      select
      componentSerialNumber,
      frm_gen_ts,
      explode(samples) as samples
      from cummins.ngca
  )level_1
)level_2