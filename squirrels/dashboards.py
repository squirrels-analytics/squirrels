from typing import Any, Literal
from abc import ABCMeta, abstractmethod
from matplotlib.figure import Figure
import io, pandas as pd

from . import _constants as c


class Dashboard(metaclass=ABCMeta):
    """
    Abstract parent class for all Dashboard classes. 
    
    Current Dashboard classes include: 
    
    [PNGDashboard, HTMLDashboard]
    """

    @property
    @abstractmethod
    def content(self) -> Any:
        """
        The contents of the dashboard
        """
        pass
    
    @property
    @abstractmethod
    def format(self) -> str:
        """
        The output format for the dashboard provided through the API response
        """
        pass


class PngDashboard(Dashboard):
    """
    Instantiate a Dashboard in PNG format from a matplotlib figure or bytes
    """
    
    def __init__(self, content: Figure | io.BytesIO | bytes) -> None:
        """
        Constructor for PNGDashboard

        Arguments:
            content: The content of the dashboard as a matplotlib.figure.Figure or bytes
        """
        if isinstance(content, Figure):
            buffer = io.BytesIO()
            content.savefig(buffer, format=c.PNG)
            content = buffer.getvalue()
        
        if isinstance(content, io.BytesIO):
            content = content.getvalue()
        
        self._content = content

    @property
    def content(self) -> bytes:
        return self._content
    
    @property
    def format(self) -> Literal['png']:
        return c.PNG
    

class HtmlDashboard(Dashboard):
    """
    Instantiate a Dashboard from an HTML string
    """

    def __init__(self, content: io.StringIO | str) -> None:
        """
        Constructor for HTMLDashboard

        Arguments:
            content: The content of the dashboard as HTML string
        """
        if isinstance(content, io.StringIO):
            content = content.getvalue()
        
        self._content = content

    @property
    def content(self) -> str:
        return self._content
    
    @property
    def format(self) -> Literal['html']:
        return c.HTML
