package log

import (
	"fmt"
	"os"
	"path/filepath"

	api "github.com/kouheiFujii/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

// 新たなセグメントを追加
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	// ファイルが存在しない場合、新たに作成する
	storeFile, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	// ファイルが存在しない場合、新たに作成する
	indexFile, err := os.OpenFile(filepath.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")), os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		// インデックスに少なくとも1つ以上のエントリがある場合、次のレコードはセグメントの最後のオフセットを使う必要がある
		// ベースオフセット + 相対オフセット + 1
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// セグメントにレコードを書き込み、新たに追加されたレコードのオフセットを返す
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	// ストアに追加
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	// インデックスエントリに追加
	if err = s.index.Write(
		// セグメント内のエントリの相対オフセット
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

// 指定されたオフセットのレコードを返す
func (s *segment) Read(off uint64) (*api.Record, error) {
	// 相対オフセットに変換し、関連するインデックスの内容を取得
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

// セグメントが最大サイズに達しているか
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes ||
		s.index.isMaxed()
}

// セグメントを閉じて、インデックスとストアのファイルを削除
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// インデックスとストアを閉じる
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}
