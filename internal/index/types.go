// Copyright 2022 Blockdaemon Inc.
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

package index

import (
	"time"

	"go.blockdaemon.com/solana/cluster-manager/types"
)

type SnapshotEntry struct {
	SnapshotKey
	Info      *types.SnapshotInfo `json:"info"`
	UpdatedAt time.Time           `json:"updated_at"`
}

type SnapshotKey struct {
	Group       string `json:"group"`
	Target      string `json:"target"`
	InverseSlot uint64 `json:"inverse_slot"` // newest-to-oldest sort
	BaseSlot    uint64 `json:"base_slot"`
}

func NewSnapshotKey(group string, target string, slot uint64, base_slot uint64) SnapshotKey {
	return SnapshotKey{
		Group:       group,
		Target:      target,
		InverseSlot: ^slot,
		BaseSlot:    base_slot,
	}
}

func (k SnapshotKey) Slot() uint64 {
	return ^k.InverseSlot
}
