select 
    office_id,
    state,
    municipality,
    office_name
from {{ ref('offices') }}