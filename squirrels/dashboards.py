import matplotlib.figure as _figure, io as _io, abc as _abc, typing as _t

from . import _constants as c


class Dashboard(metaclass=_abc.ABCMeta):
    """
    Abstract parent class for all Dashboard classes.
    """
    
    @property
    @_abc.abstractmethod
    def _content(self) -> bytes | str:
        pass
    
    @property
    @_abc.abstractmethod
    def _format(self) -> str:
        pass


class PngDashboard(Dashboard):
    """
    Instantiate a Dashboard in PNG format from a matplotlib figure or bytes
    """
    
    def __init__(self, content: _figure.Figure | _io.BytesIO | bytes) -> None:
        """
        Constructor for PngDashboard

        Arguments:
            content: The content of the dashboard as a matplotlib.figure.Figure or bytes
        """
        if isinstance(content, _figure.Figure):
            buffer = _io.BytesIO()
            content.savefig(buffer, format=c.PNG)
            content = buffer.getvalue()
        
        if isinstance(content, _io.BytesIO):
            content = content.getvalue()
        
        self.__content = content

    @property
    def _content(self) -> bytes:
        return self.__content
    
    @property
    def _format(self) -> _t.Literal['png']:
        return c.PNG
    
    def _repr_png_(self):
        return self._content
    

class HtmlDashboard(Dashboard):
    """
    Instantiate a Dashboard from an HTML string
    """

    def __init__(self, content: _io.StringIO | str) -> None:
        """
        Constructor for HtmlDashboard

        Arguments:
            content: The content of the dashboard as HTML string
        """
        if isinstance(content, _io.StringIO):
            content = content.getvalue()
        
        self.__content = content

    @property
    def _content(self) -> str:
        return self.__content
    
    @property
    def _format(self) -> _t.Literal['html']:
        return c.HTML
    
    def _repr_html_(self):
        return self._content
