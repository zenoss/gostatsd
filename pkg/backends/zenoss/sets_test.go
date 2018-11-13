package zenoss

import (
	"reflect"
	"testing"
)

func TestNewSetFromStrings(t *testing.T) {
	type args struct {
		strings []string
	}
	tests := []struct {
		name string
		args args
		want *Set
	}{
		{
			name: "empty",
			args: args{},
			want: &Set{},
		},
		{
			name: "one",
			args: args{strings: []string{"first"}},
			want: func() *Set {
				oneStringSet := &Set{}
				oneStringSet.Add("first")
				return oneStringSet
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSetFromStrings(tt.args.strings); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewSetFromStrings() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSet_UpdateFromStrings(t *testing.T) {
	type args struct {
		strings []string
	}
	tests := []struct {
		name string
		s    *Set
		args args
		want *Set
	}{
		{
			name: "empty-to-empty",
			s:    NewSetFromStrings([]string{}),
			args: args{[]string{}},
			want: &Set{},
		},
		{
			name: "empty-to-one",
			s:    NewSetFromStrings([]string{}),
			args: args{[]string{"first"}},
			want: func() *Set {
				s := &Set{}
				s.Add("first")
				return s
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s.UpdateFromStrings(tt.args.strings)
			if !reflect.DeepEqual(tt.s, tt.want) {
				t.Errorf("Set.UpdateFromStrings() = %v, want %v", tt.s, tt.want)
			}
		})
	}
}

func TestSet_Add(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name string
		s    *Set
		args args
		want *Set
	}{
		{
			name: "add first",
			s:    &Set{},
			args: args{key: "first"},
			want: NewSetFromStrings([]string{"first"}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.s.Add(tt.args.key)
			if !reflect.DeepEqual(tt.s, tt.want) {
				t.Errorf("Set.Add() = %v, want %v", tt.s, tt.want)
			}
		})
	}
}

func TestSet_Has(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name string
		s    *Set
		args args
		want bool
	}{
		{
			name: "has",
			s:    NewSetFromStrings([]string{"first"}),
			args: args{key: "first"},
			want: true,
		},
		{
			name: "doesn't have",
			s:    NewSetFromStrings([]string{"first"}),
			args: args{key: "second"},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Has(tt.args.key); got != tt.want {
				t.Errorf("Set.Has() = %v, want %v", got, tt.want)
			}
		})
	}
}
