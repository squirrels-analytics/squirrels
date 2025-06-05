from typing import Type, TypeVar, Callable, Coroutine, Any
from enum import Enum
from dataclasses import dataclass
from pydantic import BaseModel, Field
import matplotlib.figure as figure
import os, time, io, abc, typing

from ._arguments.run_time_args import DashboardArgs
from ._py_module import PyModule
from ._manifest import AnalyticsOutputConfig
from ._exceptions import InvalidInputError, ConfigurationError, FileExecutionError
from . import _constants as c, _utils as u


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


T = TypeVar('T', bound=Dashboard)


class DashboardFormat(Enum):
    PNG = "png"
    HTML = "html"

class DashboardDependencies(BaseModel):
    name: str
    dataset: str
    fixed_parameters: list[dict[str, str]] = Field(default_factory=list)

class DashboardConfig(AnalyticsOutputConfig):
    format: DashboardFormat = Field(default=DashboardFormat.PNG)
    depends_on: list[DashboardDependencies] = Field(default_factory=list)


@dataclass
class DashboardDefinition:
    dashboard_name: str
    filepath: str
    config: DashboardConfig

    @property
    def dashboard_func(self) -> Callable[[DashboardArgs], Coroutine[Any, Any, Dashboard]]:
        if not hasattr(self, '_dashboard_func'):
            module = PyModule(self.filepath)
            self._dashboard_func = module.get_func_or_class(c.MAIN_FUNC)
        return self._dashboard_func

    def get_dashboard_format(self) -> str:
        return self.config.format.value
    
    async def get_dashboard(self, args: DashboardArgs, *, dashboard_type: Type[T] = Dashboard) -> T:
        try:
            dashboard = await self.dashboard_func(args)
            assert isinstance(dashboard, dashboard_type), f"Function does not return expected Dashboard type: {dashboard_type}"
        except (InvalidInputError, ConfigurationError, FileExecutionError) as e:
            raise e
        except Exception as e:
            raise FileExecutionError(f'Failed to run "{c.MAIN_FUNC}" function for dashboard "{self.dashboard_name}"', e) from e

        return dashboard


class DashboardsIO:

    @classmethod
    def load_files(cls, logger: u.Logger, base_path: str) -> dict[str, DashboardDefinition]:
        start = time.time()
        
        dashboards_by_name = {}
        for dp, _, filenames in os.walk(u.Path(base_path, c.DASHBOARDS_FOLDER)):
            for file in filenames:
                filepath = os.path.join(dp, file)
                file_stem, extension = os.path.splitext(file)
                if not extension == '.py':
                    continue
                
                # Check for corresponding .yml file
                yml_path = os.path.join(dp, file_stem + '.yml')
                config_dict = u.load_yaml_config(yml_path) if os.path.exists(yml_path) else {}
                config = DashboardConfig(name=file_stem, **config_dict)
                dashboards_by_name[file_stem] = DashboardDefinition(file_stem, filepath, config)
                
        logger.log_activity_time("loading files for dashboards", start)
        return dashboards_by_name
