"""
Validação - CLIMA (INMET)
=========================

Regras específicas para dados climáticos do INMET.
"""

import pandas as pd
from typing import List, Optional

from src.validation.engine import (
    ValidationEngine,
    ValidationConfig,
    ValidationResult,
    ValidationRule,
    ValidationError,
    NotEmptyRule,
    RequiredColumnsRule,
    NotNullRule,
    NumericRangeRule,
    NoDuplicatesRule,
    DataTypeRule,
)


# =========================
# REGRAS ESPECÍFICAS CLIMA
# =========================

class TemperaturaValidaRule(ValidationRule):
    """Valida que temperatura está em range realista para Brasil."""
    
    @property
    def name(self) -> str:
        return "temperatura_valida"
    
    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        errors = []
        
        # Procura colunas de temperatura (podem ter nomes diferentes)
        temp_cols = [col for col in df.columns if 'TEMP' in col.upper()]
        
        for col in temp_cols:
            data = pd.to_numeric(df[col], errors='coerce')
            
            # Temperatura no Brasil: -15°C a 50°C
            fora_range = ((data < -15) | (data > 50)) & data.notna()
            count = fora_range.sum()
            
            if count > 0:
                errors.append(ValidationError(
                    rule=self.name,
                    column=col,
                    message=f"Coluna '{col}' tem {count} valores fora do range (-15°C a 50°C)",
                    rows_affected=count,
                    severity="WARNING"  # Warning porque pode ser dado válido extremo
                ))
        
        return errors


class UmidadeValidaRule(ValidationRule):
    """Valida que umidade está entre 0 e 100%."""
    
    @property
    def name(self) -> str:
        return "umidade_valida"
    
    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        errors = []
        
        umid_cols = [col for col in df.columns if 'UMID' in col.upper()]
        
        for col in umid_cols:
            data = pd.to_numeric(df[col], errors='coerce')
            
            fora_range = ((data < 0) | (data > 100)) & data.notna()
            count = fora_range.sum()
            
            if count > 0:
                errors.append(ValidationError(
                    rule=self.name,
                    column=col,
                    message=f"Coluna '{col}' tem {count} valores fora do range (0-100%)",
                    rows_affected=count
                ))
        
        return errors


# =========================
# FUNÇÃO PRINCIPAL
# =========================

def get_validation_rules() -> List[ValidationRule]:
    """Retorna lista de regras de validação para dados climáticos."""
    return [
        # Regras básicas
        NotEmptyRule(),
        
        # Colunas obrigatórias (ajuste conforme seu CSV)
        RequiredColumnsRule([
            "Data",
            "Hora UTC",
        ]),
        
        # Valores não nulos
        NotNullRule(["Data", "Hora UTC"]),
        
        # Regras específicas do clima
        TemperaturaValidaRule(),
        UmidadeValidaRule(),
    ]


def validate_dataframe(df: pd.DataFrame, fail_on_error: bool = True) -> ValidationResult:
    """
    Valida um DataFrame de dados climáticos.
    
    Args:
        df: DataFrame a ser validado
        fail_on_error: Se True, levanta exceção em caso de erro
    
    Returns:
        ValidationResult
    
    Exemplo:
        df = pd.read_parquet("s3://bucket/raw/clima/...")
        result = validate_dataframe(df)
        
        if result.is_valid:
            print("Dados OK!")
        else:
            print(result.errors)
    """
    config = ValidationConfig(
        pipeline_name="Clima INMET",
        fail_on_error=fail_on_error,
    )
    
    engine = ValidationEngine(config)
    rules = get_validation_rules()
    
    return engine.run(df, rules)


def validate_file(file_path: str, fail_on_error: bool = True) -> ValidationResult:
    """
    Valida um arquivo Parquet de dados climáticos.
    
    Args:
        file_path: Caminho do arquivo (local ou S3)
        fail_on_error: Se True, levanta exceção em caso de erro
    
    Returns:
        ValidationResult
    """
    df = pd.read_parquet(file_path)
    return validate_dataframe(df, fail_on_error=fail_on_error)