from typing import Union
from matplotlib.figure import Figure
import io


class Dashboard:
    def __init__(self, content: Union[Figure, io.BytesIO, bytes, io.StringIO, str]) -> None:
        if isinstance(content, Figure):
            buffer = io.BytesIO()
            content.savefig(buffer, format="png")
            content = buffer.getvalue()
        
        if isinstance(content, io.BytesIO):
            content = content.getvalue()
        
        if isinstance(content, io.StringIO):
            content = content.getvalue()
        
        assert isinstance(content, (bytes, str))
        self._content = content

    @property
    def content(self):
        return self._content
    
    @property
    def format(self):
        if isinstance(self.content, bytes):
            return "png"
        elif isinstance(self.content, str):
            return "html"
        else:
            raise NotImplementedError("Format cannot be derived based on dashboard content")
