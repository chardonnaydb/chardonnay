use flatbuf::rangeserver_flatbuffers::range_server::*;
use flatbuffers::FlatBufferBuilder;
use uuid::Uuid;

use crate::full_range_id::FullRangeId;

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

pub fn serialize_range_id<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    id: &FullRangeId,
) -> flatbuffers::WIPOffset<flatbuf::rangeserver_flatbuffers::range_server::RangeId<'a>> {
    let range_id = Some(Uuidu128::create(fbb, &serialize_uuid(id.range_id)));
    let keyspace_id = Some(Uuidu128::create(fbb, &serialize_uuid(id.keyspace_id.id)));
    RangeId::create(
        fbb,
        &RangeIdArgs {
            keyspace_id,
            range_id,
        },
    )
}
