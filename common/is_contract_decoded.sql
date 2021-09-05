Select
    name,
    address,
    base,
    dynamic,
    updated_at,
    created_at,
    abi,
    code
from
    polygon.contracts
where
    namespace = '{{namespace}}'
