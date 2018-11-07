# processor-split-by-values

Processor for KBC, that allows users to process files and split them based on unique values in a column.

### Usage

```
{
    "definition": {
      "component": "kozuch.processor-split-by-values"
    },
    "parameters": {
      "by_column": "col12"
    }
  }
```

In the above config, input file will be split based on unique values in `col12`.

### Output

Set of .csv files split by column values.
