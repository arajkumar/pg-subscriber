package api

type Relation struct {
  SchemaName string
  TableName string
  AttNames []string
  RowFilter string
}

type Publisher interface {
  // Name of the publisher
  Name() string

  // FetchTables returns a list of tables that are part of the publication
  FetchTables() ([]Relation, error)
}
