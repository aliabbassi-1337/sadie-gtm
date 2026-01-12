from abc import ABC, abstractmethod


class LeadGenService(ABC):
    @abstractmethod
    def scrape_city(self, city: list[str])-> bool:
        pass

    """
    Scrapes cities in an entire state
    """
    @abstractmethod
    def scrape_state(self, country: str, state: str)-> bool:
        pass
