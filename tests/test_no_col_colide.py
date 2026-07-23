"""Tests for noColColide collision policies (exclude default + raise)."""

import pytest

from spark_config_mapper.utils.list_ops import (
    noColColide,
    ON_COLLISION_EXCLUDE,
    ON_COLLISION_RAISE,
)


def test_exclude_default_drops_non_key_collision():
    master = ['personid', 'name', 'age', 'date']
    other = ['name', 'value']
    out = noColColide(master, other, ['personid'])
    assert out == ['personid', 'age', 'date']


def test_exclude_explicit_same_as_default():
    master = ['personid', 'name', 'age']
    other = ['name']
    a = noColColide(master, other, ['personid'])
    b = noColColide(master, other, ['personid'], on_collision=ON_COLLISION_EXCLUDE)
    assert a == b == ['personid', 'age']


def test_index_columns_allowed_on_both_sides():
    master = ['personid', 'tenant', 'x']
    other = ['personid', 'tenant', 'y']
    out = noColColide(master, other, ['personid', 'tenant'])
    assert out == ['personid', 'tenant', 'x']


def test_raise_on_non_key_collision():
    master = ['personid', 'entries_obs', 'index_a']
    other = ['personid', 'entries_obs', 'index_b']
    with pytest.raises(ValueError, match="entries_obs"):
        noColColide(
            master, other, ['personid'],
            on_collision=ON_COLLISION_RAISE,
        )


def test_raise_allows_shared_keys_only():
    master = ['personid', 'tenant', 'index_a']
    other = ['personid', 'tenant', 'index_b']
    out = noColColide(
        master, other, ['personid', 'tenant'],
        on_collision='raise',
    )
    assert out == ['personid', 'tenant', 'index_a']


def test_raise_lists_all_collisions():
    master = ['personid', 'a', 'b', 'c']
    other = ['personid', 'a', 'b']
    with pytest.raises(ValueError) as ei:
        noColColide(master, other, ['personid'], on_collision='raise')
    msg = str(ei.value)
    assert 'a' in msg and 'b' in msg


def test_invalid_mode():
    with pytest.raises(ValueError, match="on_collision"):
        noColColide(['a'], ['b'], [], on_collision='rename')


def test_master_list_restricts():
    master = ['personid', 'name', 'age', 'secret']
    other = ['value']
    out = noColColide(
        master, other, ['personid'],
        masterList=['personid', 'name', 'age'],
    )
    assert out == ['personid', 'name', 'age']
    assert 'secret' not in out
