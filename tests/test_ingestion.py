"""
Testes Unitários - Ingestão
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


class TestIngestionEngine:
    
    def test_engine_importa(self):
        from src.ingestion.engine import DataSource, IngestionEngine, IngestionConfig
        assert DataSource is not None
        assert IngestionEngine is not None
        assert IngestionConfig is not None
    
    def test_config_default(self):
        from src.ingestion.engine import IngestionConfig
        config = IngestionConfig(
            pipeline_name="Teste",
            log_group="/teste",
            metric_namespace="teste"
        )
        assert config.pipeline_name == "Teste"
        assert config.max_workers == 10


class TestClimaIngestion:
    
    def test_importa(self):
        from src.ingestion.pipelines.clima import GoogleDriveSource, ingest_data
        assert GoogleDriveSource is not None
        assert ingest_data is not None
    
    def test_source_com_filtro(self):
        from src.ingestion.pipelines.clima import GoogleDriveSource
        source = GoogleDriveSource(year_filter="2024")
        assert source.year_filter == "2024"


class TestSolosIngestion:
    
    def test_importa(self):
        from src.ingestion.pipelines.solos import GoogleDriveSource, ingest_data
        assert GoogleDriveSource is not None


class TestAgriculturaIngestion:
    
    def test_importa(self):
        from src.ingestion.pipelines.agricultura import GoogleDriveSource, ingest_data
        assert GoogleDriveSource is not None


class TestDataSourceInterface:
    
    def test_datasource_abstrata(self):
        from src.ingestion.engine import DataSource
        with pytest.raises(TypeError):
            DataSource()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
