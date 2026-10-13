from abc import ABC, abstractmethod
from typing import Any, Dict
from snowflake.snowpark import Session
from src.utils.logger import get_logger


class BasePipeline(ABC):
    """Abstract Base Class for standard Snowpark ETL pipelines."""

    def __init__(self, session: Session, config: Dict[str, Any]):
        self.session = session
        self.config = config
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def extract(self) -> Any:
        """Extract source data frames."""
        pass

    @abstractmethod
    def transform(self, input_data: Any) -> Any:
        """Apply Snowpark DataFrame transformations."""
        pass

    @abstractmethod
    def load(self, transformed_data: Any) -> None:
        """Persist results back into Snowflake targets."""
        pass

    def run(self) -> None:
        """Executes full pipeline workflow."""
        self.logger.info("Pipeline execution started.")
        data = self.extract()
        transformed = self.transform(data)
        self.load(transformed)
        self.logger.info("Pipeline execution completed successfully.")
