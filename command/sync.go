package command

import (
	"dhs/archive"
	"dhs/extractor"
	"dhs/extractor/datahub"
	"dhs/extractor/postgresql"
	"dhs/util"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Extractor struct {
	Config string `name:"config" short:"c" type:"string" help:"Specify a JSON configuration file (ignores connection string when supplied). A file called dh-config.json will be auto-recognized if it exists." default:"./dh-config.yml" json:"config_file"`
	// Extract          []string `name:"extract" short:"x" type:"string" default:"source,datahub" enum:"source,datahub" help:"Determines what to extract, source (database/source) and/or Datahub metadata."`
	Schemas          []string `name:"schemas" short:"s" type:"string" help:"List of source schemas to extract." json:"config_schema"`
	Outfile          string   `name:"outfile" short:"o" type:"string" help:"Dump the extraction to a JSON file." json:"output_file"`
	Expand           []string `name:"expand_json" short:"e" type:"string" help:"When configured, these JSON fields are expanded so each key is treated as a unique item." json:"expand_json"`
	SkipViewExpand   bool     `name:"expand_fast" short:"f" type:"bool" default:"false" help:"Speed up JSON expansion process by ignoring views" json:"expand_fast"`
	Source           string   `name:"datahub_source" short:"i" type:"string" help:"Name or ID of the Datahub data source." json:"source"`
	DryRun           bool     `name:"dryrun" type:"bool" default:"false" help:"Pull data but do not push deltas." json:"dry_run"`
	DatahubURL       string   `name:"url" short:"u" help:"URL of the Datahub API" json:"datahub_url"`
	Max              int      `name:"max" short:"m" default:"35" help:"The maximum number of updates to preview (dry run)." json:"max"`
	System           string   `name:"system" short:"j" help:"The system/job ID where status messages are logged." json:"datahub_job_id"`
	APIKey           string   `name:"api_key" short:"k" help:"Optional API key to access the Datahub" json:"api_key"`
	Debug            bool     `name:"debug" short:"d" help:"Turn on debugging"`
	RelsOnly         bool     `name:"onlyrelationships" short:"r" help:"Only sync relationships"`
	ConnectionString string   `arg:"conn" optional:"" help:"The source connection string used to extract metadata from the data store" json:"db_connection_string"`
}

func (e *Extractor) Run(ctx *Context) error {
	start := time.Now()

	// Open the archive
	cache := archive.Open("./datahub-sync.db")

	// if util.InSlice[string]("source", e.Extract) {
	fmt.Println("Now extracting from source...\n")
	if e.ConnectionString == "" {
		_, err := os.Stat(e.Config)
		if err != nil {
			if os.IsNotExist(err) {
				err = errors.New("configuration/connection string not found")
			}
			fmt.Println(err)
			return err
		}

		cfg := NewConfig(e.Config)
		err = cfg.Apply(e)
		if err != nil {
			fmt.Println(err)
			return err
		}
	}

	if e.Debug {
		fmt.Println("  configuration applied")
		util.Dump(e)
		fmt.Println("  setting up extractor...")
	}

	remote := e.extractor()
	// remote.ApplySchemas(e.Schemas...)

	if e.Debug {
		remote.SetDebugging(true)
		fmt.Println("  extractor setup complete")
	}

	var end_json time.Duration
	var end_sqlite time.Duration
	var end_expand time.Duration

	start_extract := time.Now()

	if e.Debug {
		fmt.Println("  begin extraction...")
	}

	dh, dherr := datahub.New(e.DatahubURL, e.Source, cache, e.APIKey)
	if dherr != nil {
		fmt.Println(dherr.Error())
		os.Exit(1)
	}

	elements := []string{}
	if e.RelsOnly {
		fmt.Println("Extract rels from datasource")
		rels, err := remote.ExtractRelationships()
		if err != nil {
			fmt.Println(err)
			return err
		}

		fmt.Println("Extract rels from datahub")
		err = dh.PopulateSources()
		if err != nil {
			fmt.Println(err)
			return err
		}

		id := dh.Source()
		uri := "/catalog/relationships/source/" + id
		cd, body, err := dh.Get(uri)
		if err != nil {
			return err
		}

		if cd != 200 {
			fmt.Println(string(body))
			return errors.New(string(body))
		}

		var data map[string]interface{}
		err = json.Unmarshal(body, &data)
		if err != nil {
			return err
		}

		outdated := []string{}
		for _, rel := range data["relationships"].([]interface{}) {
			if _, ok := rels[rel.(map[string]interface{})["name"].(map[string]interface{})["physical"].(string)]; !ok {
				outdated = append(outdated, rel.(map[string]interface{})["id"].(string))
			}
		}

		if len(outdated) > 0 {
			uri := "/catalog/relationships"

			cd, _, err := dh.Delete(uri, map[string]interface{}{
				"relationships": outdated,
			})

			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}

			if cd != 200 {
				fmt.Printf("Error: HTTP Code %v received when attempting to delete relationships\n", cd)
			} else {
				fmt.Printf("  deleted %v relationship(s)\n", len(outdated))
			}
		}

		if len(rels) > 0 {
			relbody := make([]interface{}, 0)
			for _, item := range rels {
				relbody = append(relbody, item)
			}

			cd, _, err = dh.Put("/catalog/relationships", map[string]interface{}{
				"relationships": relbody,
			})

			if err != nil {
				fmt.Println(err.Error())
				os.Exit(1)
			}

			if cd != 200 {
				fmt.Printf("Error: HTTP Code %v received when attempting to delete relationships\n", cd)
			} else {
				fmt.Printf("  created/updated %v relationship(s)\n", len(rels))
			}
		}

		os.Exit(0)
	}

	doc, err := remote.Extract(elements...)
	if err != nil {
		fmt.Println(err)
		return err
	}

	if e.Debug {
		fmt.Println("  setting up document...")
	}
	cache.SetDoc(doc)

	end_extract := time.Since(start_extract)
	fmt.Printf("Source Extraction: %s\n", end_extract)

	if len(e.Expand) > 0 && (util.InSlice[string]("views", elements) || util.InSlice[string]("entities", elements)) {
		if e.Debug {
			fmt.Println("  enabling JSON field expansion functions...")
		}
		start_expand := time.Now()
		remote.ExpandJSONFields(doc, e.SkipViewExpand, e.Expand...)
		end_expand = time.Since(start_expand)
		fmt.Printf("JSON Expansion: %s\n", end_expand)
	}

	if len(elements) > 0 {
		switch strings.ToLower(filepath.Ext(e.Outfile)) {
		// JSON
		case ".json":
			start_json := time.Now()
			if e.Debug {
				fmt.Println("  writing metadoc to JSON file...")
			}
			util.DumpFile(e.Outfile, doc.ToJSON())
			end_json = time.Since(start_json)
			fmt.Printf("Created %s in %s\n", e.Outfile, end_json)
			// case ".xml":
		}
	}

	start_sqlite := time.Now()

	if util.InSlice[string]("entities", elements) || util.InSlice[string]("relationships", elements) {
		if e.Debug {
			fmt.Println("  extracting data set metadata from source...")
		}
		sets := extractor.GetAllSets(doc)
		fmt.Printf("  stashing %v set(s)...\n", len(sets))
		err = cache.UpsertSets("source", sets)
		if err != nil {
			fmt.Println(err)
		}

		if e.Debug {
			fmt.Println("  extracting data item metadata from source...")
		}
		items := extractor.GetAllItems(doc)
		fmt.Printf("  stashing %v item(s)...\n", len(items))
		err = cache.UpsertItems("source", items)
		if err != nil {
			fmt.Println(err)
		}
	}

	if util.InSlice[string]("relationships", elements) {
		if e.Debug {
			fmt.Println("  extracting data relationship metadata from source...")
		}
		rels := extractor.GetAllRelationships(doc)
		fmt.Printf("  stashing %v relationship(s)...\n", len(rels))
		err = cache.UpsertRelationships("source", rels)
		if err != nil {
			fmt.Println(err)
		}
	}

	end_sqlite = time.Since(start_sqlite)
	fmt.Printf("Created cache in %s\n", end_sqlite)
	// }

	// if util.InSlice[string]("datahub", e.Extract) {
	fmt.Println("\nNow extracting from Datahub...")
	start_datahub := time.Now()

	if dherr == nil {
		if len(elements) == 1 && elements[0] == "relationships" {
			for _, schema := range e.Schemas {
				dh.GetDoc().ApplySchemaByName(schema)
				util.Dump(dh.GetDoc().GetSchemas())
				os.Exit(1)
				// diff := archive.CreateDiff()

				// err = dh.PopulateRelationships(diff)
				// if err == nil {
				rels := extractor.GetAllRelationships(dh.GetDoc())
				fmt.Printf("  stashing %v relationship(s)...\n", len(rels))

				// err = cache.UpsertRelationships("datahub", rels)
				// if err == nil {
				// 	if e.Debug {
				// 		fmt.Println("  diffing data relationships...")
				// 	}
				// 	reldiff, err := cache.DiffRelationships(diff)
				// 	if err == nil {
				// 		if e.Debug {
				// 			fmt.Println("  diffing individual relationship joins...")
				// 		}
				// 		joindiff, err := cache.DiffJoins(diff, reldiff)
				// 		if err == nil {
				// 			fmt.Printf("\nNow syncing with the Datahub...\n")
				// 			if e.DryRun {
				// 				if e.Debug {
				// 					fmt.Println("  running dry run...")
				// 				}
				// 				dh.DryRun(reldiff, e.Max, "relationship")
				// 				fmt.Println("")
				// 				dh.DryRun(joindiff, e.Max, "join")
				// 			} else {
				// 				if e.Debug {
				// 					fmt.Println("  syncing...")
				// 				}
				// 				dh.DryRun(reldiff, e.Max, "relationship")
				// 				dh.Commit(reldiff)
				// 				cache.ResetDatahub()
				// 				cache.ResetDatasource()
				// 			}
				// 		} else {
				// 			fmt.Println(err)
				// 		}
				// 	} else {
				// 		fmt.Println(err)
				// 	}
				// } else {
				// 	fmt.Println(err)
				// }
				// } else {
				// 	fmt.Println(err)
				// }
			}
		} else {
			if e.Debug {
				fmt.Println("  populating datahub sources...")
			}
			err = dh.PopulateSources()
			if err != nil {
				fmt.Println(err)
			} else {
				sets := extractor.GetAllSets(dh.GetDoc())
				fmt.Printf("  stashing %v set(s)...\n", len(sets))
				err := cache.UpsertSets("datahub", sets)
				if err != nil {
					fmt.Println(err)
				} else {
					if e.Debug {
						fmt.Println("  diffing sets...")
					}
					diff, err := cache.DiffSets()

					if err == nil {
						if e.Debug {
							fmt.Println("  populating data items...")
						}
						err = dh.PopulateItems(diff)
						if err == nil {
							items := extractor.GetAllItems(dh.GetDoc())
							fmt.Printf("  stashing %v item(s)...\n", len(items))
							err := cache.UpsertItems("datahub", items)
							if err == nil {
								if e.Debug {
									fmt.Println("  diffing data items...")
								}
								itemdiff, err := cache.DiffItems(diff)
								if err == nil {
									if e.Debug {
										fmt.Println("  populating datahub relationships...")
									}
									err = dh.PopulateRelationships(diff)
									if err == nil {
										rels := extractor.GetAllRelationships(dh.GetDoc())
										fmt.Printf("  stashing %v relationship(s)...\n", len(rels))

										err = cache.UpsertRelationships("datahub", rels)
										if err == nil {
											if e.Debug {
												fmt.Println("  diffing data relationships...")
											}
											reldiff, err := cache.DiffRelationships(diff)
											if err == nil {
												if e.Debug {
													fmt.Println("  diffing individual relationship joins...")
												}
												joindiff, err := cache.DiffJoins(diff, reldiff)
												if err == nil {
													fmt.Printf("\nNow syncing with the Datahub...\n")
													if e.DryRun {
														if e.Debug {
															fmt.Println("  running dry run...")
														}
														dh.DryRun(diff, e.Max)
														fmt.Println("")
														dh.DryRun(itemdiff, e.Max, "item")
														fmt.Println("")
														dh.DryRun(reldiff, e.Max, "relationship")
														fmt.Println("")
														dh.DryRun(joindiff, e.Max, "join")
													} else {
														if e.Debug {
															fmt.Println("  syncing...")
														}
														dh.DryRun(diff, e.Max)
														dh.Commit(diff)
														fmt.Println("")
														dh.DryRun(itemdiff, e.Max, "item")
														dh.Commit(itemdiff)
														fmt.Println("")
														dh.DryRun(reldiff, e.Max, "relationship")
														dh.Commit(reldiff)
														cache.ResetDatahub()
														cache.ResetDatasource()
													}
												} else {
													fmt.Println(err)
												}
											} else {
												fmt.Println(err)
											}
										} else {
											fmt.Println(err)
										}
									} else {
										fmt.Println(err)
									}
								} else {
									fmt.Println(err)
								}
							} else {
								fmt.Println(err)
							}
						} else {
							fmt.Println(err)
						}
					} else {
						fmt.Println(err)
					}
				}
			}
		}
	} else {
		fmt.Println(dherr.Error())
	}

	end_datahub := time.Since(start_datahub)
	fmt.Printf("Datahub Extraction: %s\n", end_datahub)
	// }

	end := time.Since(start)

	fmt.Printf("Total Duration: %s\n", end)

	return nil
}

func (e *Extractor) extractor() extractor.Extractor {
	schema := strings.Split(e.ConnectionString, ":")[0]

	if strings.ToLower(schema) == "postgres" || strings.ToLower(schema) == "greenplum" {
		schema = "postgresql"
	}

	var empty extractor.Extractor

	switch strings.ToLower(schema) {
	case "postgresql":
		return postgresql.New(e.ConnectionString, e.Schemas)
	case "greenplum":
		return postgresql.New(e.ConnectionString, e.Schemas)
	default:
		panic(schema + " extractor not found")
	}

	return empty
}
