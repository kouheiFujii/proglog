## proto

```golang
syntax = "proto3";

package log.v1;

option go_package = "https://github.com/kouheiFujii/api/log_v1";

message Record {
  bytes value = 1;
  uint64 offset = 2;
}
```

syntax: protobuf構文を proto3 に指定

package: 生成される pacakge 名

option: 使用している言語によって指定できる option 異なる
https://github.com/protocolbuffers/protobuf/blob/master/src/google/protobuf/descriptor.proto

go_package: 生成されたファイルを import する path を指定

message: golangの構造体と同じ。以下の形式で表記される

```
message 構造体名 {
  フィールド型 フィールド名 = フィールドID
}
```
