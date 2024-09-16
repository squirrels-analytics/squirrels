from typing import Type, TypeVar, Callable, Coroutine, Any
from dataclasses import dataclass
import inspect, os

from .arguments.run_time_args import DashboardArgs
from ._timer import timer, time
from ._py_module import PyModule
from . import _constants as c, _utils as u, dashboards as d

T = TypeVar('T', bound=d.Dashboard)


@dataclass
class DashboardFunction:
    dashboard_name: str
    filepath: str

    @property
    def dashboard_func(self) -> Callable[[DashboardArgs], Coroutine[Any, Any, d.Dashboard]]:
        if not hasattr(self, '_dashboard_func'):
            module = PyModule(self.filepath)
            self._dashboard_func = module.get_func_or_class(c.MAIN_FUNC)
        return self._dashboard_func

    def get_dashboard_format(self) -> str:
        return_type = inspect.signature(self.dashboard_func).return_annotation
        assert issubclass(return_type, d.Dashboard), f"Function must return Dashboard type"
        if return_type == d.PngDashboard:
            return c.PNG
        elif return_type == d.HtmlDashboard:
            return c.HTML
        else:
            raise NotImplementedError(f"Dashboard format {return_type} not supported")
    
    async def get_dashboard(self, args: DashboardArgs, *, dashboard_type: Type[T] = d.Dashboard) -> T:
        try:
            dashboard = await self.dashboard_func(args)
            assert isinstance(dashboard, dashboard_type), f"Function does not return expected Dashboard type: {dashboard_type}"
        except (u.InvalidInputError, u.ConfigurationError, u.FileExecutionError) as e:
            raise e
        except Exception as e:
            raise u.FileExecutionError(f'Failed to run "{c.MAIN_FUNC}" function for dashboard "{self.dashboard_name}"', e) from e

        return dashboard


class DashboardsIO:

    @classmethod
    def load_files(cls, base_path: str) -> dict[str, DashboardFunction]:
        start = time.time()
        
        dashboards_by_name = {}
        for dp, _, filenames in os.walk(u.Path(base_path, c.DASHBOARDS_FOLDER)):
            for file in filenames:
                filepath = os.path.join(dp, file)
                file_stem, extension = os.path.splitext(file)
                if extension == '.py':
                    dashboards_by_name[file_stem] = DashboardFunction(file_stem, filepath)
        
        timer.add_activity_time("loading files for dashboards", start)
        return dashboards_by_name
