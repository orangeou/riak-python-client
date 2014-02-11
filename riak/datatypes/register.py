from collections import Sized
from riak.datatypes.datatype import Datatype


class Register(Sized, Datatype):
    """
    A convergent datatype that represents an opaque string that is set
    with last-write-wins semantics, and may only be embedded in
    :class:`~riak.datatypes.Map` instances.
    """

    _value = ""
    _new_value = None
    _type_error_msg = "Registers can only be strings"

    @Datatype.dirty_value.getter
    def dirty_value(self):
        """
        Returns the value of the register with local mutation applied.

        :rtype: str
        """
        if self._new_value is not None:
            return self._new_value[:]
        else:
            return self._value

    @Datatype.value.getter
    def value(self):
        """
        Returns a copy of the original value of the register.

        :rtype: str
        """
        return self._value[:]

    def to_op(self):
        """
        Extracts the mutation operation from the register.

        :rtype: str, None
        """
        if self._new_value is not None:
            return self._new_value

    def assign(self, new_value):
        """
        Assigns a new value to the register.

        :param new_value: the new value for the register
        :type new_value: str
        """
        self._raise_if_badtype(new_value)
        self._new_value = new_value

    def __len__(self):
        return len(self.value)

    @classmethod
    def _check_value(new_value):
        return isinstance(new_value, basestring)
