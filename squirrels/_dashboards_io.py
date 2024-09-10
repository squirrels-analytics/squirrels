from typing import Callable, Coroutine, Any
from dataclasses import dataclass
import inspect, os

from .arguments.run_time_args import DashboardArgs
from ._timer import timer, time
from ._py_module import PyModule
from . import _constants as c, _utils as u, dashboards as d


@dataclass
class DashboardFunction:
    dashboard_name: str
    filepath: str
    auto_reload: bool

    @property
    def dashboard_func(self) -> Callable[[DashboardArgs], Coroutine[Any, Any, d._Dashboard]]:
        if self.auto_reload or not hasattr(self, "_dashboard_func"):
            module = PyModule(self.filepath)
            self._dashboard_func = module.get_func_or_class(c.MAIN_FUNC)
        return self._dashboard_func

    def get_dashboard_format(self) -> str:
        return_type = inspect.signature(self.dashboard_func).return_annotation
        assert issubclass(return_type, d._Dashboard), f"Function must return Dashboard type"
        if return_type == d.PngDashboard:
            return c.PNG
        elif return_type == d.HtmlDashboard:
            return c.HTML
        else:
            raise NotImplementedError(f"Dashboard format {return_type} not supported")
    
    async def get_dashboard(self, args: DashboardArgs) -> d._Dashboard:
        try:
            dashboard = await self.dashboard_func(args)
            assert isinstance(dashboard, d._Dashboard), f"Function must return Dashboard type"
        except (u.InvalidInputError, u.ConfigurationError, u.FileExecutionError) as e:
            raise e
        except Exception as e:
            raise u.FileExecutionError(f'Failed to run "{c.MAIN_FUNC}" function for dashboard "{self.dashboard_name}"', e) from e

        return dashboard


class DashboardsIO:
    dashboards_by_name: dict[str, DashboardFunction]

    @classmethod
    def load_files(cls, *, auto_reload = False) -> dict[str, DashboardFunction]:
        start = time.time()
        
        for dp, _, filenames in os.walk(c.DASHBOARDS_FOLDER):
            for file in filenames:
                filepath = os.path.join(dp, file)
                file_stem, extension = os.path.splitext(file)
                if extension == '.py':
                    cls.dashboards_by_name[file_stem] = DashboardFunction(file_stem, filepath, auto_reload)
        
        timer.add_activity_time("loading files for dashboards", start)
        return cls.dashboards_by_name

    @classmethod
    def get_dashboard_format(cls, dashboard_name: str) -> str:
        dashboard_func = cls.dashboards_by_name[dashboard_name]
        return dashboard_func.get_dashboard_format()
    
    @classmethod
    async def get_dashboard(cls, dashboard_name: str, args: DashboardArgs) -> d._Dashboard:
        dashboard_func = cls.dashboards_by_name[dashboard_name]
        return await dashboard_func.get_dashboard(args)
