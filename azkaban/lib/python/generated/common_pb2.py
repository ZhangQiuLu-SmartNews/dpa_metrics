# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: common.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
from google.protobuf import descriptor_pb2
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='common.proto',
  package='recsys',
  syntax='proto3',
  serialized_pb=_b('\n\x0c\x63ommon.proto\x12\x06recsys\"\x18\n\x06Vector\x12\x0e\n\x06values\x18\x01 \x03(\x02\x62\x06proto3')
)




_VECTOR = _descriptor.Descriptor(
  name='Vector',
  full_name='recsys.Vector',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='values', full_name='recsys.Vector.values', index=0,
      number=1, type=2, cpp_type=6, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=24,
  serialized_end=48,
)

DESCRIPTOR.message_types_by_name['Vector'] = _VECTOR
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

Vector = _reflection.GeneratedProtocolMessageType('Vector', (_message.Message,), dict(
  DESCRIPTOR = _VECTOR,
  __module__ = 'common_pb2'
  # @@protoc_insertion_point(class_scope:recsys.Vector)
  ))
_sym_db.RegisterMessage(Vector)


# @@protoc_insertion_point(module_scope)