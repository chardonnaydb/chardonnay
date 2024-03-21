use flatbuf::rangeserver_flatbuffers::range_server::*;
use uuid::Uuid;

pub fn deserialize_uuid(uuidf: Uuidu128<'_>) -> Uuid {
    let res: u128 = ((uuidf.upper() as u128) << 64) | (uuidf.lower() as u128);
    Uuid::from_u128_le(res)
}

pub fn serialize_uuid(uuid: Uuid) -> Uuidu128Args {
    let uint128 = uuid.to_u128_le();
    let lower = uint128 as u64;
    let upper = (uint128 >> 64) as u64;
    Uuidu128Args { lower, upper }
}
