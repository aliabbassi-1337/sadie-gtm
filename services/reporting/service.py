from abc import ABC, abstractmethod


class IService(ABC):
    """Reporting Service - Generate and deliver reports to stakeholders."""

    @abstractmethod
    async def export_city(self, city: str, state: str) -> str:
        """
        Generate Excel report for a city.
        Returns file path of generated report.
        """
        pass

    @abstractmethod
    async def export_state(self, state: str) -> str:
        """
        Generate Excel report for an entire state.
        Returns file path of generated report.
        """
        pass

    @abstractmethod
    async def upload_to_onedrive(self, file_path: str) -> bool:
        """
        Upload file to OneDrive.
        Returns True if successful.
        """
        pass

    @abstractmethod
    async def send_slack_notification(self, message: str, channel: str = "#leads") -> bool:
        """
        Send notification to Slack channel.
        Returns True if successful.
        """
        pass

    @abstractmethod
    async def get_exportable_hotels_count(self, city: str = None, state: str = None) -> int:
        """
        Count hotels ready for export (status=5, live).
        Optionally filter by city or state.
        """
        pass


class Service(IService):
    def __init__(self) -> None:
        pass

    async def export_city(self, city: str, state: str) -> str:
        # TODO: Generate Excel for city
        return ""

    async def export_state(self, state: str) -> str:
        # TODO: Generate Excel for state
        return ""

    async def upload_to_onedrive(self, file_path: str) -> bool:
        # TODO: Integrate OneDrive upload
        return False

    async def send_slack_notification(self, message: str, channel: str = "#leads") -> bool:
        # TODO: Integrate Slack API
        return False

    async def get_exportable_hotels_count(self, city: str = None, state: str = None) -> int:
        # TODO: Query hotels WHERE status=5
        return 0
