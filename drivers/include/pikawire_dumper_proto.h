/* Wire protocol between pikawire (parent) and pikawire-dumper (sidecar).
 * Parent writes fixed binary requests to the child's stdin; the child
 * replies with length-prefixed frames on stdout. Little-endian throughout.
 *
 * request (stdin, every request is a fixed 8192-byte frame):
 *   'O' u16 dir_len <dir> u16 db_len <db> i32 batch_num i64 scan_batch
 *       u32 type_mask i32 scan_strategy i64 list_tail_n u16 pattern_len <pat>
 *       u8 resume_type_len <resume_type> u16 resume_key_len <resume_key>
 *       (resume: skip stages before <resume_type>, continue that stage from
 *       <resume_key> INCLUSIVE — re-emitting that key's full image is
 *       idempotent; lengths 0 mean "from the start")
 *   'N'                       -> reply: record_batch frame
 *   'C'                       -> exit
 * reply (stdout), every reply is: u32 total_len <payload>
 *   open ack payload:  u8 status (0 ok, 1 error) [u16 len <msg>]
 *   next batch payload: u32 count { u16 type_len <type> u16 key_len <key>
 *                                   u32 resp_len <raw_resp> }*count
 *     count==0 => stream exhausted
 */
#define PIKAWIRE_DUMP_PROTO_VERSION 1u
