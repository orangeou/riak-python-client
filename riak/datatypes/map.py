from collections import Mapping
from riak.util import lazy_property
from riak.datatypes.datatype import Datatype
from riak.datatypes.counter import Counter
from riak.datatypes.flag import Flag
from riak.datatypes.register import Register
from riak.datatypes.set import Set


class TypedMapView(Mapping):
    """
    Implements a sort of view over a Map, filtered by the embedded
    datatype.
    """

    def __init__(self, parent, datatype):
        self.map = parent
        self.datatype = datatype

    # Mapping API
    def __getitem__(self, key):
        return self.map[(key, self.datatype)]

    def __iter__(self):
        for key in self.map.value:
            name, datatype = key
            if datatype == self.datatype:
                yield self.map[key]

    def __len__(self):
        return len(iter(self))

    # From the MutableMapping API
    def __delitem__(self, key):
        del self.map[(key, self.datatype)]

    def add(self, key):
        """
        Adds an empty value for the given key.
        """
        self.map.add((key, self.datatype),)


class Map(Mapping, Datatype):
    """
    A convergent datatype that acts as a key-value datastructure. Keys
    are pairs of `(name, datatype)` where `name` is a string and
    `datatype` is the datatype name. Values are other convergent
    datatypes, represented by any concrete type in this module.

    You cannot set values in the map directly (it does not implement
    `__setitem__`), but you may add new empty values or access
    non-existing values directly via bracket syntax. If a key is not
    in the original value of the map when accessed, fetching the key
    will cause its associated value to be created. The two lines in
    the example below are equivalent, assuming that the key does not
    previously exist in the map::

        map[('name', 'register')]
        map.add(('name', 'register'))

    Keys and their associated values may be deleted from the map as
    you would in a dict::

        del map[('emails', 'set')]

    Convenience accessors exist that partition the map's keys by
    datatype and implement the :class:`~collections.Mapping`
    behavior as well as supporting deletion::

        map.sets['emails']
        map.registers['name']
        del map.counters['likes']
    """
    _value = {}
    _removes = set()
    _updates = {}
    _adds = set()
    _type_error_msg = "Map must be a dict with (name, type) keys"

    @lazy_property
    def counters(self):
        """
        Filters keys in the map to only those of counter types. Example::

            map.counters['views'].increment()
            map.counters.add('likes')  # adds an empty counter to the map
            del map.counters['points']
        """
        return TypedMapView(self, 'counter')

    @lazy_property
    def flags(self):
        """
        Filters keys in the map to only those of flag types. Example::

            map.flags['confirmed'].enable()
            map.flags.add('admin')  # adds an uninitialized (False)
                                    # flag to the map
            del map.flags['attending']
        """
        return TypedMapView(self, 'flag')

    @lazy_property
    def maps(self):
        """
        Filters keys in the map to only those of map types. Example::

            map.maps['emails'].registers['home'].set("user@example.com")
            map.maps.add('addresses') # adds an empty nested map to the map
            del map.maps['spam']
        """
        return TypedMapView(self, 'map')

    @lazy_property
    def registers(self):
        """
        Filters keys in the map to only those of register types. Example::

            map.registers['username'].set_value("riak-user")
            map.registers.add('phone') # adds an empty register to the map
            del map.registers['access_key']
        """
        return TypedMapView(self, 'register')

    @lazy_property
    def sets(self):
        """
        Filters keys in the map to only those of set types. Example::

            map.sets['friends'].add("brett")
            map.sets.add('followers') # adds an empty set to the map
            del map.sets['favorites']
        """
        return TypedMapView(self, 'set')

    def __contains__(self, key):
        """
        A map contains a key if that key exists in the original value
        or has been added or mutated.

        :rtype: bool
        """
        self._check_key(key)
        return (key in self._value) or (key in self._updates)

    # collections.Mapping API
    def __getitem__(self, key):
        """
        Fetches a convergent datatype at the given key.

        .. note: If the key is not in the map, a new empty datatype
           will be inserted at that key and returned. If the key was
           previously deleted, that mutation will be discarded.

        :param key: the key of the value to fetch
        :type key: tuple
        :rtype: :class:`Datatype` matching the datatype in the key
        """
        self._check_key(key)
        self._removes.discard(key)
        if key in self._value:
            return self._value[key]
        else:
            # If the key does not exist, we assume they are wanting to
            # create a new one with that name/type.
            self.add(key)
            return self._updates[key]

    def __iter__(self):
        """
        Iterates over the *immutable* original value of the map. Use
        the :attr:`dirty_value` to iterate over mutated values.
        """
        return iter(self.value)

    def __len__(self):
        """
        Returns the size of the original value of the map. Use the
        :attr:`dirty_value` to account for local mutations.
        """
        return len(self._value)

    def __delitem__(self, key):
        """
        Deletes a key from the map. If you have previously mutated the
        datatype associated with this key, those mutations will be
        discarded.

        .. note: You may delete keys that are not entries in the map.
           If the Riak server does not find the entry in the set, an
           error may be returned to the client. For safety, always
           submit removal operations with a context.

        :param key: the key to remove
        :type key: tuple
        """
        # NB: deleting a key only marks it deleted, and you can delete
        # things that don't appear in the value!
        self._check_key(key)
        self._adds.discard(key)
        del self._updates[key]
        self._removes.add(key)

    def add(self, key):
        """
        Adds an empty datatype for the given key. That datatype can be
        later mutated with type-specific operations. If the key is
        already in the map, this only has the effect of asserting that
        the key exists.
        """
        self._check_key(key)
        if not key in self.value:
            self._updates[key] = TYPES[key[1]]()
        self._removes.discard(key)
        self._adds.add(key)

    def _check_key(self, key):
        """
        Ensures well-formedness of a key.
        """
        if not len(key) == 2:
            raise TypeError('invalid key: %r' % key)
        elif key[1] not in TYPES:
            raise TypeError('invalid datatype: %s'. key[1])

    # Datatype API
    @Datatype.dirty_value.getter
    def dirty_value(self):
        """
        A representation of the set with local mutations applied.
        Nested values will include their mutations as well.

        :rtype: dict
        """
        dvalue = {}
        for key in self._value:
            dvalue[key] = self._value[key].dirty_value()
        for key in self._updates:
            dvalue[key] = self._updates[key].dirty_value()
        for key in self._removes:
            del dvalue[key]
        return dvalue

    @Datatype.value.getter
    def value(self):
        """
        Returns a copy of the original map's value. Nested values are
        pure Python values as returned by :attr:`Datatype.value` from
        the nested types.

        :rtype: dict
        """
        pvalue = {}
        for key in self._value:
            pvalue[key] = self._value[key].value()
        return pvalue

    def to_op(self):
        """
        Extracts the modfication operation(s) from the map.

        :rtype: list, None
        """
        adds = [('add', a) for a in self._adds]
        removes = [('remove', r) for r in self._removes]
        value_updates = list(self._extract_updates(self._value))
        new_updates = list(self._extract_updates(self._updates))
        all_updates = adds + removes + value_updates + new_updates
        if all_updates:
            return all_updates
        else:
            return None

    @classmethod
    def _check_type(self, value):
        for key in value:
            try:
                self._check_key(key)
            except:
                return False
        return True

    @classmethod
    def _coerce_value(new_value):
        cvalue = {}
        for key in new_value:
            cvalue[key] = TYPES[key[1]](new_value[key])
        return cvalue

    def _extract_updates(self, d):
        for key in d:
            op = d[key].to_op()
            if op is not None:
                yield ('update', key, op)

#: A dict from type names as strings to the class that implements
#: them. This is used inside :class:`Map` to initialize new values.
TYPES = {
    'counter': Counter,
    'flag': Flag,
    'map': Map,
    'register': Register,
    'set': Set
}
