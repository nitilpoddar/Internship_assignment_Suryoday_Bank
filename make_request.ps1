param (
    [string]$name,
    [int]$age
)

curl -X POST -H 'Content-Type: application/json' -d '{'name': $name , 'age': $age}'
