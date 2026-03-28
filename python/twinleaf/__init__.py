from twinleaf import _twinleaf

class Device(_twinleaf._Device):
    """ Primary TIO interface with sensor object """
    def __new__(cls, url=None, route=None, announce=False, instantiate=True):
        device = super().__new__(cls, url, route)
        return device

    def __init__(self, url=None, route=None, announce=False, instantiate=True):
        super().__init__()
        if instantiate:
            self._instantiate_rpcs()
            self._instantiate_samples(announce)

    def __repr__(self):
        try:
            dev_info = self._rpc('dev.serial', b'').decode()
        except RuntimeError:
            dev_info = ''
        return f"{self.__module__}.{self.__class__.__name__}('{dev_info}', url='{self._url}', route='{self._route}'"

    def _rpc_int(self, name: str, size: int, signed: bool, value: int | None = None) -> int:
        """ Use struct to send int-typed RPCs """
        import struct
        match size, signed:
            case 1, True: fstr = '<b'
            case 2, True: fstr = '<h'
            case 4, True: fstr = '<i'
            case 8, True: fstr = '<q'
            case 1, False: fstr = '<B'
            case 2, False: fstr = '<H'
            case 4, False: fstr = '<I'
            case 8, False: fstr = '<Q'
        payload = b'' if value is None else struct.pack(fstr, value)
        rep = self._rpc(name, payload)
        val = struct.unpack(fstr, rep)[0]
        return val

    def _rpc_float(self, name: str, size: int, value: float | None = None) -> float:
        """ Use struct to send float-typed RPCs """
        import struct
        fstr = '<f' if (size == 4) else '<d'
        payload = b'' if value is None else struct.pack(fstr, value)
        rep = self._rpc(name, payload)
        val = struct.unpack(fstr, rep)[0]
        return val

    def _instantiate_rpcs(self):
        """ Set up Device.samples, then recursively instantiate RPCs """
        self._registry = self._rpc_registry()
        self.settings = _RpcSurvey('settings')
        self._instantiate_rpcs_recursive(self.settings)

    def _instantiate_rpcs_recursive(self, parent, prefix=''):
        """ Get children from registry, setattr them, then recurse """
        for child_name in self._registry.children_of(prefix):
            full_path = f'{prefix}.{child_name}' if prefix else child_name
            rpc = self._registry.find(full_path)
            attr_name = '_rpc' if child_name == 'rpc' else child_name

            if rpc is not None:
                child = _Rpc(rpc, self)
            else:
                child = _RpcSurvey(attr_name)
            setattr(parent, attr_name, child)
            self._instantiate_rpcs_recursive(child, full_path)

    def _samples_dict(self, n: int = 1, stream: str = "", columns: list[str] | None=None) -> dict[int, dict[str, list[int | float]]]:
        """ Parse underlying sample iterator into dict """
        if columns is None: columns = [] # Avoid mutable default
        samples = list(self._samples(n, stream=stream, columns=columns))
        # Bin into streams
        streams = {}
        for line in samples:
            stream_id = line.pop("stream", None)
            if stream_id not in streams:
                streams[stream_id] = { "stream": stream_id }
            for key, value in line.items():
                if key not in streams[stream_id]:
                    streams[stream_id][key] = []
                streams[stream_id][key].append(value)
        return streams

    def _samples_list(self, n: int = 1, stream: str = "", columns: list[str] | None=None, time_column=True, title_row=True) -> list[list[str | int | float]]:
        """ Parse underlying sample iterator into tabular array """
        if columns is None: columns = [] # Avoid mutable default
        streams = self._samples_dict(n, stream, columns)
        # Convert to list with rows of data. Not super happy about how inefficient this is. 
        if len(streams.items()) > 1:
            raise NotImplementedError("Stream concatenation not yet implemented for two different streams")
        stream_dict = list(streams.values())[0]
        stream_dict.pop('stream')
        if not time_column:
            stream_dict.pop('time')
        data_columns = [column for column in stream_dict.values() ]
        data_rows = [list(row) for row in zip(*data_columns)]
        if title_row:
            column_names = list(stream_dict.keys())
            data_rows.insert(0,column_names)
        return data_rows

    def _instantiate_samples(self, announce: bool=False):
        metadata = self._get_metadata()
        if announce:
            dev_meta = metadata['device']
            print(f"{dev_meta['name']} ({dev_meta['serial_number']}) [{dev_meta['firmware_hash']}]")

        streams_flattened = []
        for stream, value in metadata['streams'].items():
            for column_name in value['columns'].keys():
                streams_flattened.append(stream+"."+column_name)

        # All samples
        self.samples = _SamplesDict(self, "samples", stream="", columns=[])

        for stream_column in streams_flattened:
            stream, *prefix, mname = stream_column.split(".")
            parent = self.samples

            # All samples for this stream
            if not hasattr(parent, stream):
                setattr(parent, stream, _SamplesList(self, name=stream, stream=stream, columns=[]))
            parent = getattr(parent, stream)

            # Wildcard columns within stream
            stream_prefix = ""
            for token in prefix:
                stream_prefix += "." + token
                if not hasattr(parent, token):
                    setattr(parent, token, _SamplesList(self, token, stream, columns=[stream_prefix[1:]+".*"]))
                parent = getattr(parent, token)

            # Specific stream samples
            stream, column_name = stream_column.split(".",1)
            setattr(parent, mname, _SamplesList(self, mname, stream, columns=[column_name]))

    def _interact(self):
        imported_objects = {}
        imported_objects["tl"] = self
        try:
            import IPython
            IPython.embed(
                user_ns=imported_objects, 
                banner1="", 
                banner2="", # Use   : {self._shortname}.<tab>
                exit_msg="",
                enable_tip=False)
        except ImportError:
            import code
            repl = code.InteractiveConsole(locals=imported_objects)
            repl.interact(
                banner = "", 
                exitmsg = "")

type _rpc_type = int | float | str | bytes | None
class _RpcNode:
    """ Base class for RPCs and surveys in the device tree """
    def __init__(self, name: str):
        self.__name__ = name

    def __repr__(self):
        return f"{self.__module__}.{self.__class__.__name__}('{self.__name__}')"

    def _survey(self) -> dict[str, _rpc_type]:
        """ Recursively collect all readable RPC values in this subtree """
        results = {}
        for name, attr in self.__dict__.items():
            if isinstance(attr, _RpcNode):
                # Check if it's an RPC that should be read
                if isinstance(attr, _Rpc):
                    if attr._readable and attr._type not in { None, bytes }:
                        results[attr.__name__] = attr._call()

                # Recursively survey children (works for both Rpc and Survey)
                results |= attr._survey()
        return results

class _RpcSurvey(_RpcNode):
    """" Branch object that can collect all callable child RPC values """
    def __init__(self, name: str):
        super().__init__(name)

    def __call__(self) -> dict[str, _rpc_type]:
        return self._survey()

class _Rpc(_RpcNode):
    """ Base class for RPCs """
    def __new__(cls, pyrpc: _twinleaf._Rpc, device: Device):
        match pyrpc:
            case r if r.type_str == '' and r.size_bytes != 0:
                subclass = _RpcReadWrite # unknown/bytes rpc
            case r if r.readable and r.writable:
                subclass = _RpcReadWrite
            case r if r.writable:
                subclass = _RpcWriteOnly
            case r if r.readable:
                subclass = _RpcReadOnly
            case _:
                subclass = _RpcAction
        rpc = super().__new__(subclass)
        return rpc

    def __init__(self, pyrpc: _twinleaf._Rpc, device: Device):
        super().__init__(pyrpc.name)
        self._device = device
        self._data_size = pyrpc.size_bytes
        self._readable  = pyrpc.readable
        self._writable  = pyrpc.writable
        self._type: type | None = None
        match pyrpc.type_str:
            case t if t.startswith('u'):
                self._type = int
                self._data_type = 0
                self._signed = False
            case t if t.startswith('i'):
                self._type = int
                self._data_type = 1
                self._signed = True
            case t if t.startswith('f'):
                self._type = float
                self._data_type = 2
            case t if t.startswith('s'):
                self._type = str
                self._data_type = 3
            case '' if self._data_size == 0:
                self._type = None
                self._data_type = 0
            case other:
                self._type = bytes
                self._data_type = 0

    def __repr__(self):
        ret = super().__repr__().strip(')') + ", "
        if hasattr(self, '_signed') and not self._signed:
            ret += "u"
        ret += self._type.__name__
        if self._data_size: # is not 0 or None
            ret += str(self._data_size*8)
        ret += ')'
        return ret

    def _call(self, arg: _rpc_type=None) -> _rpc_type:
        match self._type:
            case t if t is int:
                return self._device._rpc_int(self.__name__, self._data_size, self._signed, arg)
            case t if t is float:
                return self._device._rpc_float(self.__name__, self._data_size, arg)
            case t if t is str:
                if arg is None: arg = ''
                return self._device._rpc(self.__name__, arg.encode()).decode()
            case t if t is bytes:
                if arg is None: arg = b''
                return self._device._rpc(self.__name__, arg)
            case None:
                return self._device._rpc(self.__name__, b'')
            case other:
                raise TypeError(f"Invalid RPC type {other}, RPC types must be {_rpc_type}")

class _RpcReadOnly(_Rpc):
    def __call__(self):
        return self._call()

class _RpcWriteOnly(_Rpc):
    def __call__(self, arg):
        return self._call(arg)

class _RpcReadWrite(_Rpc):
    def __call__(self, arg=None):
        return self._call(arg)

class _RpcAction(_Rpc):
    def __call__(self) -> None:
        return self._call()

# Samples classes
class _SamplesBase:
    """ Base class for sample objects """
    def __init__(self, device: Device, name: str, stream: str, columns: list[str]):
        self._device = device
        self.__name__ = name
        self._stream = stream
        self._columns = columns

    def __repr__(self):
        return f"{self.__module__}.{self.__class__.__name__}('{self.__name__}', stream='{self._stream}', columns={self._columns})"

class _SamplesDict(_SamplesBase):
    """ Returns samples as dict keyed by stream_id """
    def __init__(self, device: Device, name: str, stream: str="", columns: list[str] | None=None):
        super().__init__(device, name, stream, columns if columns is not None else [] )

    def __call__(self, n: int=1, *, time_column=True, title_row=True):
        return self._device._samples_dict(n, self._stream, self._columns, time_column=time_column, title_row=title_row)

class _SamplesList(_SamplesBase):
    """ Returns samples as list for single stream """
    def __init__(self, device: Device, name: str, stream: str="", columns: list[str] | None=None):
        super().__init__(device, name, stream, columns if columns is not None else [] )

    def __call__(self, n: int=1, *, time_column=True, title_row=True):
        return self._device._samples_list(n, self._stream, self._columns, time_column=time_column, title_row=title_row)
