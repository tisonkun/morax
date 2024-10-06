// Copyright 2024 tison <wander4096@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::error;
use std::fmt::Display;
use std::io;
use std::mem::size_of;

use byteorder::BigEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;

use crate::records::Header;
use crate::records::Record;
use crate::IoResult;

pub(crate) fn err_codec_message<E>(message: E) -> io::Error
where
    E: Into<Box<dyn error::Error + Send + Sync>>,
{
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(crate) fn err_decode_message_unsupported(version: i16, schemata: &str) -> io::Error {
    err_codec_message(format!("failed to read version {version} of {schemata}"))
}

pub(crate) fn err_encode_message_unsupported(version: i16, schemata: &str) -> io::Error {
    err_codec_message(format!("failed to write version {version} of {schemata}"))
}

pub(crate) fn err_decode_message_null(field: impl Display) -> io::Error {
    err_codec_message(format!("non-nullable field {field} was serialized as null"))
}

pub(crate) fn err_encode_message_null(field: impl Display) -> io::Error {
    err_codec_message(format!(
        "non-nullable field {field} to be serialized as null"
    ))
}

pub(crate) trait Decoder<T: Sized> {
    fn decode<B: ReadBytesExt>(&self, buf: &mut B) -> IoResult<T>;
}

pub(crate) trait Encoder<T> {
    fn encode<B: WriteBytesExt>(&self, buf: &mut B, value: T) -> IoResult<()>;
    fn calculate_size(&self, value: T) -> usize;
}

pub(crate) trait FixedSizeEncoder {
    const SIZE: usize;
}

pub trait Decodable: Sized {
    fn read<B: ReadBytesExt>(buf: &mut B, version: i16) -> IoResult<Self>;
}

pub trait Encodable: Sized {
    fn write<B: WriteBytesExt>(&self, buf: &mut B, version: i16) -> IoResult<()>;
    fn calculate_size(&self, version: i16) -> usize;
}

macro_rules! define_ints_codec {
    ($name:ident, $ty:ty, $write:ident, $read:ident $(,)? $($endian:ident)?) => {
        #[derive(Debug, Copy, Clone)]
        pub(crate) struct $name;

        impl Decoder<$ty> for $name {
            fn decode<B: ReadBytesExt>(&self, buf: &mut B) -> IoResult<$ty> {
                buf.$read$(::<$endian>)?()
            }
        }

        impl Encoder<$ty> for $name {
            fn encode<B: WriteBytesExt>(&self, buf: &mut B, value: $ty) -> IoResult<()> {
                buf.$write$(::<$endian>)?(value)
            }

            #[inline]
            fn calculate_size(&self, _: $ty) -> usize {
                Self::SIZE
            }
        }

        impl Encoder<&$ty> for $name {
            fn encode<B: WriteBytesExt>(&self, buf: &mut B, value: &$ty) -> IoResult<()> {
                self.encode(buf, *value)
            }

            #[inline]
            fn calculate_size(&self, _: &$ty) -> usize {
                Self::SIZE
            }
        }

        impl FixedSizeEncoder for $name {
            const SIZE: usize = size_of::<$ty>();
        }
    };
}

define_ints_codec!(Int8, i8, write_i8, read_i8);
define_ints_codec!(Int16, i16, write_i16, read_i16, BigEndian);
define_ints_codec!(Int32, i32, write_i32, read_i32, BigEndian);
define_ints_codec!(Int64, i64, write_i64, read_i64, BigEndian);
define_ints_codec!(UInt8, u8, write_u8, read_u8);
define_ints_codec!(UInt16, u16, write_u16, read_u16, BigEndian);
define_ints_codec!(UInt32, u32, write_u32, read_u32, BigEndian);
define_ints_codec!(UInt64, u64, write_u64, read_u64, BigEndian);
define_ints_codec!(Float32, f32, write_f32, read_f32, BigEndian);
define_ints_codec!(Float64, f64, write_f64, read_f64, BigEndian);

#[derive(Debug, Copy, Clone)]
pub(crate) struct Bool;

impl Decoder<bool> for Bool {
    fn decode<B: ReadBytesExt>(&self, buf: &mut B) -> IoResult<bool> {
        Ok(buf.read_u8()? != 0)
    }
}

impl Encoder<bool> for Bool {
    fn encode<B: WriteBytesExt>(&self, buf: &mut B, value: bool) -> IoResult<()> {
        buf.write_u8(if value { 1 } else { 0 })
    }

    #[inline]
    fn calculate_size(&self, _: bool) -> usize {
        Self::SIZE
    }
}

impl FixedSizeEncoder for Bool {
    const SIZE: usize = size_of::<bool>();
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct Uuid;

impl Decoder<uuid::Uuid> for Uuid {
    fn decode<B: ReadBytesExt>(&self, buf: &mut B) -> IoResult<uuid::Uuid> {
        read_uuid(buf)
    }
}

impl Encoder<uuid::Uuid> for Uuid {
    fn encode<B: WriteBytesExt>(&self, buf: &mut B, value: uuid::Uuid) -> IoResult<()> {
        write_uuid(buf, value)
    }

    fn calculate_size(&self, _: uuid::Uuid) -> usize {
        Self::SIZE
    }
}

impl FixedSizeEncoder for Uuid {
    const SIZE: usize = 16;
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct VarInt;

impl Decoder<i32> for VarInt {
    fn decode<B: ReadBytesExt>(&self, buf: &mut B) -> IoResult<i32> {
        read_unsigned_varint(buf)
    }
}

impl Encoder<i32> for VarInt {
    fn encode<B: WriteBytesExt>(&self, buf: &mut B, value: i32) -> IoResult<()> {
        write_unsigned_varint(buf, value)
    }

    fn calculate_size(&self, value: i32) -> usize {
        let mut res = 1;
        let mut v = value;
        while v >= 0x80 {
            res += 1;
            v >>= 7;
        }
        debug_assert!(res <= 5);
        res
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct VarLong;

impl Decoder<i64> for VarLong {
    fn decode<B: ReadBytesExt>(&self, buf: &mut B) -> IoResult<i64> {
        read_unsigned_varlong(buf)
    }
}

impl Encoder<i64> for VarLong {
    fn encode<B: WriteBytesExt>(&self, buf: &mut B, value: i64) -> IoResult<()> {
        write_unsigned_varlong(buf, value)
    }

    fn calculate_size(&self, value: i64) -> usize {
        let mut res = 1;
        let mut v = value;
        while v >= 0x80 {
            res += 1;
            v >>= 7;
        }
        debug_assert!(res <= 10);
        res
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct NullableString(pub bool /* flexible */);

impl Decoder<Option<String>> for NullableString {
    fn decode<B: ReadBytesExt>(&self, buf: &mut B) -> IoResult<Option<String>> {
        let len = if self.0 {
            VarInt.decode(buf)? - 1
        } else {
            Int16.decode(buf)? as i32
        };
        Ok(read_bytes(buf, len)?.map(|bs| String::from_utf8_lossy(&bs).into_owned()))
    }
}

impl Encoder<Option<&str>> for NullableString {
    fn encode<B: WriteBytesExt>(&self, buf: &mut B, value: Option<&str>) -> IoResult<()> {
        write_str(buf, value, self.0)
    }

    fn calculate_size(&self, value: Option<&str>) -> usize {
        let bytes = value.map(|s| s.as_bytes());
        let len = bytes.map(|bs| bs.len()).unwrap_or(0);
        if self.0 {
            VarInt.calculate_size(len as i32 + 1) + len
        } else {
            Int16::SIZE + len
        }
    }
}

impl Encoder<&str> for NullableString {
    fn encode<B: WriteBytesExt>(&self, buf: &mut B, value: &str) -> IoResult<()> {
        self.encode(buf, Some(value))
    }

    fn calculate_size(&self, value: &str) -> usize {
        self.calculate_size(Some(value))
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct NullableBytes(pub bool /* flexible */);

impl Decoder<Option<Vec<u8>>> for NullableBytes {
    fn decode<B: ReadBytesExt>(&self, buf: &mut B) -> IoResult<Option<Vec<u8>>> {
        let len = if self.0 {
            VarInt.decode(buf)? - 1
        } else {
            Int32.decode(buf)?
        };
        read_bytes(buf, len)
    }
}

impl<T: AsRef<[u8]>> Encoder<Option<&T>> for NullableBytes {
    fn encode<B: WriteBytesExt>(&self, buf: &mut B, value: Option<&T>) -> IoResult<()> {
        write_bytes(buf, value.map(|s| s.as_ref()), self.0)
    }

    fn calculate_size(&self, value: Option<&T>) -> usize {
        let bytes = value.map(|s| s.as_ref());
        let len = bytes.map(|bs| bs.len()).unwrap_or(0);
        if self.0 {
            VarInt.calculate_size(len as i32 + 1) + len
        } else {
            Int32::SIZE + len
        }
    }
}

impl<T: AsRef<[u8]>> Encoder<&T> for NullableBytes {
    fn encode<B: WriteBytesExt>(&self, buf: &mut B, value: &T) -> IoResult<()> {
        self.encode(buf, Some(value))
    }

    fn calculate_size(&self, value: &T) -> usize {
        self.calculate_size(Some(value))
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct Struct(pub i16 /* version */);

impl<T: Decodable> Decoder<T> for Struct {
    fn decode<B: ReadBytesExt>(&self, buf: &mut B) -> IoResult<T> {
        T::read(buf, self.0)
    }
}

impl<T: Encodable> Encoder<&T> for Struct {
    fn encode<B: WriteBytesExt>(&self, buf: &mut B, value: &T) -> IoResult<()> {
        value.write(buf, self.0)
    }

    fn calculate_size(&self, value: &T) -> usize {
        value.calculate_size(self.0)
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct NullableArray<E>(pub E, pub bool /* flexible */);

impl<T, E: Decoder<T>> Decoder<Option<Vec<T>>> for NullableArray<E> {
    fn decode<B: ReadBytesExt>(&self, buf: &mut B) -> IoResult<Option<Vec<T>>> {
        let len = if self.1 {
            VarInt.decode(buf)? - 1
        } else {
            Int32.decode(buf)?
        };
        match len {
            -1 => Ok(None),
            n if n >= 0 => {
                let n = n as usize;
                let mut result = Vec::with_capacity(n);
                for _ in 0..n {
                    result.push(self.0.decode(buf)?);
                }
                Ok(Some(result))
            }
            n => Err(err_codec_message(format!("invalid length: {n}"))),
        }
    }
}

impl<T, E: for<'a> Encoder<&'a T>> Encoder<Option<&[T]>> for NullableArray<E> {
    fn encode<B: WriteBytesExt>(&self, buf: &mut B, value: Option<&[T]>) -> IoResult<()> {
        match value {
            None => {
                if self.1 {
                    VarInt.encode(buf, 0)
                } else {
                    Int32.encode(buf, -1)
                }
            }
            Some(s) => self.encode(buf, s),
        }
    }

    fn calculate_size(&self, value: Option<&[T]>) -> usize {
        match value {
            None => {
                if self.1 {
                    VarInt.calculate_size(0)
                } else {
                    Int32::SIZE
                }
            }
            Some(ns) => {
                let mut res = 0;
                res += if self.1 {
                    VarInt.calculate_size(ns.len() as i32 + 1)
                } else {
                    Int32::SIZE
                };
                for n in ns {
                    res += self.0.calculate_size(n);
                }
                res
            }
        }
    }
}

impl<T, E: for<'a> Encoder<&'a T>> Encoder<&[T]> for NullableArray<E> {
    fn encode<B: WriteBytesExt>(&self, buf: &mut B, value: &[T]) -> IoResult<()> {
        if self.1 {
            VarInt.encode(buf, value.len() as i32 + 1)?;
        } else {
            Int32.encode(buf, value.len() as i32)?;
        }
        for v in value {
            self.0.encode(buf, v)?;
        }
        Ok(())
    }

    fn calculate_size(&self, value: &[T]) -> usize {
        self.calculate_size(Some(value))
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct RawTaggedFieldList;

impl Decoder<Vec<RawTaggedField>> for RawTaggedFieldList {
    fn decode<B: ReadBytesExt>(&self, buf: &mut B) -> IoResult<Vec<RawTaggedField>> {
        RawTaggedFieldList.decode_with(buf, |_, _, _| Ok(false))
    }
}

impl Encoder<&[RawTaggedField]> for RawTaggedFieldList {
    fn encode<B: WriteBytesExt>(&self, buf: &mut B, fields: &[RawTaggedField]) -> IoResult<()> {
        self.encode_with(buf, 0, fields, |_| Ok(()))
    }

    fn calculate_size(&self, fields: &[RawTaggedField]) -> usize {
        self.calculate_size_with(0, 0, fields)
    }
}

impl RawTaggedFieldList {
    pub(crate) fn decode_with<B: ReadBytesExt, F>(
        &self,
        buf: &mut B,
        mut f: F,
    ) -> IoResult<Vec<RawTaggedField>>
    where
        F: FnMut(&mut B, i32, usize) -> IoResult<bool>,
    {
        let n = VarInt.decode(buf)?;
        let mut res = vec![];
        for _ in 0..n {
            let tag = VarInt.decode(buf)?;
            let size = VarInt.decode(buf)? as usize;
            let consumed = f(buf, tag, size)?;
            if !consumed {
                match read_bytes(buf, size as i32)? {
                    None => return Err(err_codec_message("unexpected null data")),
                    Some(data) => res.push(RawTaggedField { tag, data }),
                }
            }
        }
        Ok(res)
    }

    pub(crate) fn encode_with<B: WriteBytesExt, F>(
        &self,
        buf: &mut B,
        n: usize, // extra fields
        fields: &[RawTaggedField],
        mut f: F,
    ) -> IoResult<()>
    where
        F: FnMut(&mut B) -> IoResult<()>,
    {
        VarInt.encode(buf, (fields.len() + n) as i32)?;
        f(buf)?;
        for field in fields {
            RawTaggedFieldWriter.write_byte_buffer(buf, field.tag, &field.data)?;
        }
        Ok(())
    }

    pub(crate) fn calculate_size_with(
        &self,
        n: usize,  // extra fields
        bs: usize, // extra bytes
        fields: &[RawTaggedField],
    ) -> usize {
        let mut res = 0;
        res += VarInt.calculate_size((fields.len() + n) as i32);
        for field in fields {
            res += VarInt.calculate_size(field.tag);
            res += VarInt.calculate_size(field.data.len() as i32);
            res += field.data.len();
        }
        res + bs
    }
}

#[derive(Debug, Default, Clone)]
pub struct RawTaggedField {
    pub tag: i32,
    pub data: Vec<u8>,
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct RawTaggedFieldWriter;

impl RawTaggedFieldWriter {
    pub(crate) fn write_field<
        B: WriteBytesExt,
        T: Copy, // primitive or reference
        E: Encoder<T>,
    >(
        &self,
        buf: &mut B,
        tag: i32,
        encoder: E,
        value: T,
    ) -> IoResult<()> {
        VarInt.encode(buf, tag)?;
        VarInt.encode(buf, encoder.calculate_size(value) as i32)?;
        encoder.encode(buf, value)?;
        Ok(())
    }

    pub(crate) fn calculate_field_size<T, E: Encoder<T>>(
        &self,
        tag: i32,
        encoder: E,
        value: T,
    ) -> usize {
        let size = encoder.calculate_size(value);
        let mut res = 0;
        res += VarInt.calculate_size(tag);
        res += VarInt.calculate_size(size as i32);
        res + size
    }

    fn write_byte_buffer<B: WriteBytesExt>(
        &self,
        buf: &mut B,
        tag: i32,
        bs: &[u8],
    ) -> IoResult<()> {
        VarInt.encode(buf, tag)?;
        VarInt.encode(buf, bs.len() as i32)?;
        buf.write_all(bs)?;
        Ok(())
    }
}

fn read_uuid<B: ReadBytesExt>(buf: &mut B) -> IoResult<uuid::Uuid> {
    let msb = buf.read_u64::<BigEndian>()?;
    let lsb = buf.read_u64::<BigEndian>()?;
    Ok(uuid::Uuid::from_u64_pair(msb, lsb))
}

fn write_uuid<B: WriteBytesExt>(buf: &mut B, n: uuid::Uuid) -> IoResult<()> {
    buf.write_all(n.as_ref())
}

fn read_unsigned_varint<B: ReadBytesExt>(buf: &mut B) -> IoResult<i32> {
    let mut res = 0;
    for i in 0.. {
        debug_assert!(i < 5); // no larger than i32
        let next = buf.read_u8()? as i32;
        res |= (next & 0x7F) << (i * 7);
        if next < 0x80 {
            break;
        }
    }
    Ok(res)
}

fn read_unsigned_varlong<B: ReadBytesExt>(buf: &mut B) -> IoResult<i64> {
    let mut res = 0;
    for i in 0.. {
        debug_assert!(i < 10); // no larger than i64
        let next = buf.read_u8()? as i64;
        res |= (next & 0x7F) << (i * 7);
        if next < 0x80 {
            break;
        }
    }
    Ok(res)
}

fn varint_zigzag(i: i32) -> i32 {
    (((i as u32) >> 1) as i32) ^ -(i & 1)
}

fn varlong_zigzag(i: i64) -> i64 {
    (((i as u64) >> 1) as i64) ^ -(i & 1)
}

fn read_varint<B: ReadBytesExt>(buf: &mut B) -> IoResult<i32> {
    read_unsigned_varint(buf).map(varint_zigzag)
}

fn read_varlong<B: ReadBytesExt>(buf: &mut B) -> IoResult<i64> {
    read_unsigned_varlong(buf).map(varlong_zigzag)
}

fn write_unsigned_varint<B: WriteBytesExt>(buf: &mut B, n: i32) -> IoResult<()> {
    let mut v = n;
    while v >= 0x80 {
        buf.write_u8((v as u8) | 0x80)?;
        v >>= 7;
    }
    buf.write_u8(v as u8)
}

fn write_unsigned_varlong<B: WriteBytesExt>(buf: &mut B, n: i64) -> IoResult<()> {
    let mut v = n;
    while v >= 0x80 {
        buf.write_u8((v as u8) | 0x80)?;
        v >>= 7;
    }
    buf.write_u8(v as u8)
}

fn read_bytes<B: ReadBytesExt>(buf: &mut B, len: i32) -> IoResult<Option<Vec<u8>>> {
    match len {
        -1 => Ok(None),
        n if n >= 0 => {
            let n = n as usize;
            let mut v = vec![0; n];
            buf.read_exact(&mut v).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("failed to read {n} bytes: {e}"),
                )
            })?;
            Ok(Some(v))
        }
        n => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid length: {n}"),
        )),
    }
}

fn write_str<B: WriteBytesExt>(buf: &mut B, str: Option<&str>, flexible: bool) -> IoResult<()> {
    match str {
        None => {
            if flexible {
                VarInt.encode(buf, 0)?
            } else {
                Int16.encode(buf, -1)?
            }
        }
        Some(bs) => {
            let bs = bs.as_bytes();
            let len = bs.len();
            if flexible {
                VarInt.encode(buf, len as i32 + 1)?;
            } else {
                Int16.encode(buf, len as i16)?;
            }
            buf.write_all(bs)?;
        }
    }
    Ok(())
}

fn write_bytes<B: WriteBytesExt>(
    buf: &mut B,
    bytes: Option<&[u8]>,
    flexible: bool,
) -> IoResult<()> {
    match bytes {
        None => {
            if flexible {
                VarInt.encode(buf, 0)?
            } else {
                Int32.encode(buf, -1)?
            }
        }
        Some(bs) => {
            let len = bs.len() as i32;
            if flexible {
                VarInt.encode(buf, len + 1)?;
            } else {
                Int32.encode(buf, len)?;
            }
            buf.write_all(bs)?;
        }
    }
    Ok(())
}

#[derive(Debug, Copy, Clone)]
pub(crate) struct RecordList;

impl Decoder<Vec<Record>> for RecordList {
    fn decode<B: ReadBytesExt>(&self, buf: &mut B) -> IoResult<Vec<Record>> {
        struct Entry(i32, Option<Vec<u8>>);
        fn read_key_value<B: ReadBytesExt>(buf: &mut B) -> IoResult<(Entry, Entry)> {
            let key = {
                let len = read_varint(buf)?;
                let payload = read_bytes(buf, len)?;
                Entry(len, payload)
            };

            let value = {
                let len = read_varint(buf)?;
                let payload = read_bytes(buf, len)?;
                Entry(len, payload)
            };

            Ok((key, value))
        }

        let cnt = Int32.decode(buf)?;
        let mut records = vec![];
        for _ in 0..cnt {
            let mut record = Record {
                len: read_varint(buf)?,
                attributes: Int8.decode(buf)?,
                timestamp_delta: read_varlong(buf)?,
                offset_delta: read_varint(buf)?,
                ..Default::default()
            };

            let (key, value) = read_key_value(buf)?;
            record.key_len = key.0;
            record.key = key.1;
            record.value_len = value.0;
            record.value = value.1;

            let headers_cnt = read_varint(buf)?;
            for _ in 0..headers_cnt {
                let (key, value) = read_key_value(buf)?;
                record.headers.push(Header {
                    key_len: key.0,
                    key: key.1,
                    value_len: value.0,
                    value: value.1,
                });
            }
            records.push(record);
        }
        Ok(records)
    }
}
