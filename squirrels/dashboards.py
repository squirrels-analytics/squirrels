from typing import Any, Literal
from abc import ABCMeta, abstractmethod
import matplotlib.figure, io

from . import _constants as c


class _Dashboard(metaclass=ABCMeta):
    """
    Abstract parent class for all Dashboard classes.
    """
    
    @property
    @abstractmethod
    def _content(self) -> Any:
        pass
    
    @property
    @abstractmethod
    def _format(self) -> str:
        pass


class PngDashboard(_Dashboard):
    """
    Instantiate a Dashboard in PNG format from a matplotlib figure or bytes
    """
    
    def __init__(self, content: matplotlib.figure.Figure | io.BytesIO | bytes) -> None:
        """
        Constructor for PngDashboard

        Arguments:
            content: The content of the dashboard as a matplotlib.figure.Figure or bytes
        """
        if isinstance(content, matplotlib.figure.Figure):
            buffer = io.BytesIO()
            content.savefig(buffer, format=c.PNG)
            content = buffer.getvalue()
        
        if isinstance(content, io.BytesIO):
            content = content.getvalue()
        
        self.content = content

    @property
    def _content(self) -> bytes:
        return self.content
    
    @property
    def _format(self) -> Literal['png']:
        return c.PNG
    

class HtmlDashboard(_Dashboard):
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
        
        self.content = content

    @property
    def _content(self) -> str:
        return self.content
    
    @property
    def _format(self) -> Literal['html']:
        return c.HTML
