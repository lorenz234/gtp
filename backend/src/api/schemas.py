from typing import List, Dict, Optional, Union, Any
from pydantic import BaseModel, ConfigDict

# --- 1. Shared Types & Aliases ---

DataType = Optional[Union[float, int, str]]
RowData = List[DataType]

class JsonTable(BaseModel):
    """Standard table: {types: [...], data: [[...], [...]]}"""
    types: List[str]
    data: List[RowData]

class JsonTableSingleValue(BaseModel):
    """Summary/Point data: {types: [...], data: [...]}"""
    types: List[str]
    data: RowData

class DynamicJsonTable(BaseModel):
    """
    Format: {types: [...], 1d: [...], 7d: [...]}
    Allows arbitrary keys.
    """
    types: List[str]
    model_config = ConfigDict(extra='allow')

class BaseResponse(BaseModel):
    """Common footer for all API responses"""
    last_updated_utc: str

# --- 2. Metric Responses ---

class MetricDetails(BaseModel):
    metric_id: str
    metric_name: str
    # Dictionaries allow dynamic keys (daily, weekly, etc.) without rigid classes
    timeseries: Dict[str, Optional[JsonTable]] 
    changes: Dict[str, Optional[DynamicJsonTable]]
    summary: Dict[str, JsonTableSingleValue]

class MetricResponse(BaseResponse):
    """Output for /metrics/{level}/{origin_key}/{metric_id}"""
    details: MetricDetails

# --- 3. Chain Overview Components ---

class RankingItem(BaseModel):
    rank: int
    out_of: int
    color_scale: float
    value: Optional[float] = None
    value_usd: Optional[float] = None
    value_eth: Optional[float] = None

class KpiCard(BaseModel):
    sparkline: JsonTable
    current_values: JsonTableSingleValue
    wow_change: JsonTableSingleValue

class AchievementItem(BaseModel):
    level: Optional[int] = None
    total_value: Optional[float] = None
    percent_to_next_level: Optional[float] = None

class StreakItem(BaseModel):
    streak_length: int
    yesterday_value: float

class ChainAchievements(BaseModel):
    streaks: Dict[str, Dict[str, StreakItem]] 
    lifetime: Dict[str, Dict[str, AchievementItem]] 

class ChainEcosystem(BaseModel):
    active_apps: Dict[str, int]
    apps: JsonTable

class ChainData(BaseModel):
    chain_id: str
    chain_name: str
    highlights: List[Dict[str, Any]]
    events: List[Any]
    ranking: Dict[str, RankingItem]
    kpi_cards: Dict[str, KpiCard]
    achievements: ChainAchievements
    blockspace: Dict[str, Union[JsonTable, List]]
    ecosystem: Union[ChainEcosystem, Dict]

class ChainOverviewResponse(BaseResponse):
    """Output for /chains/{origin_key}/overview"""
    data: ChainData

# --- 4. Other Endpoints ---

class StreaksResponse(BaseResponse):
    """Output for /chains/all/streaks_today"""
    data: Dict[str, Dict[str, Dict[str, float]]]

class EcosystemResponse(BaseResponse):
    """Output for /ecosystem/builders"""
    # SIMPLIFICATION: Removed EcosystemBuilderData wrapper.
    # We define data as a Dict where the value matches the ChainEcosystem structure.
    data: Dict[str, ChainEcosystem]

class UserInsightsData(BaseModel):
    new_user_contracts: Dict[str, JsonTable]
    cross_chain_addresses: Dict[str, JsonTable]

class UserInsightsResponse(BaseResponse):
    """Output for /chains/{origin_key}/user_insights"""
    data: UserInsightsData

# --- 5. Blockspace Tree Map ---

class TreeMapResponse(BaseResponse):
    """Output for /blockspace/tree_map"""
    data: JsonTable
