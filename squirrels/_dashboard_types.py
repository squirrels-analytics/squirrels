import matplotlib.figure as figure, io, abc, typing

from . import _constants as c


class Dashboard(metaclass=abc.ABCMeta):
    """
    Abstract parent class for all Dashboard classes.
    """
    
    @property
    @abc.abstractmethod
    def _content(self) -> bytes | str:
        pass
    
    @property
    @abc.abstractmethod
    def _format(self) -> str:
        pass


class PngDashboard(Dashboard):
    """
    Instantiate a Dashboard in PNG format from a matplotlib figure or bytes
    """
    
    def __init__(self, content: figure.Figure | io.BytesIO | bytes) -> None:
        """
        Constructor for PngDashboard

        Arguments:
            content: The content of the dashboard as a matplotlib.figure.Figure or bytes
        """
        if isinstance(content, figure.Figure):
            buffer = io.BytesIO()
            content.savefig(buffer, format=c.PNG)
            content = buffer.getvalue()
        
        if isinstance(content, io.BytesIO):
            content = content.getvalue()
        
        self.__content = content

    @property
    def _content(self) -> bytes:
        return self.__content
    
    @property
    def _format(self) -> typing.Literal['png']:
        return c.PNG
    
    def _repr_png_(self):
        return self._content
    

class HtmlDashboard(Dashboard):
    """
    Instantiate a Dashboard from an HTML string
    """

    def __init__(self, content: io.StringIO | str) -> None:
        """
        Constructor for HtmlDashboard

        Arguments:
            content: The content of the dashboard as HTML string
        """
        if isinstance(content, io.StringIO):
            content = content.getvalue()
        
        self.__content = content

    @property
    def _content(self) -> str:
        return self.__content
    
    @property
    def _format(self) -> typing.Literal['html']:
        return c.HTML
    
    def _repr_html_(self):
        return self._content
