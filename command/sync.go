package command

import (
	"dhs/archive"
	"dhs/extractor"
	"dhs/extractor/datahub"
	"dhs/extractor/postgresql"
	"dhs/util"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Extractor struct {
	Config string `name:"config" short:"c" type:"string" help:"Specify a JSON configuration file (ignores connection string when supplied). A file called dh-config.json will be auto-recognized if it exists." default:"./dh-config.yml"`
	// Extract          []string `name:"extract" short:"x" type:"string" default:"source,datahub" enum:"source,datahub" help:"Determines what to extract, source (database/source) and/or Datahub metadata."`
	Schemas          []string `name:"schemas" short:"s" type:"string" help:"List of source schemas to extract."`
	Outfile          string   `name:"outfile" short:"o" type:"string" help:"Dump the extraction to a JSON file."`
	Expand           []string `name:"expand_json" short:"e" type:"string" help:"When configured, these JSON fields are expanded so each key is treated as a unique item."`
	SkipViewExpand   bool     `name:"expand_fast" short:"f" type:"bool" default:"false" help:"Speed up JSON expansion process by ignoring views"`
	Source           string   `name:"datahub_source" short:"d" type:"string" help:"Name or ID of the Datahub data source."`
	DryRun           bool     `name:"dryrun" type:"bool" default:"false" help:"Pull data but do not push deltas."`
	DatahubURL       string   `name:"url" short:"u" help:"URL of the Datahub API"`
	Max              int      `name:"max" short:"m" default:"35" help:"The maximum number of updates to preview (dry run)."`
	System           string   `name:"system" short:"j" help:"The system/job ID where status messages are logged."`
	APIKey           string   `name:"api_key" short:"k" help:"Optional API key to access the Datahub"`
	ConnectionString string   `arg:"conn" optional:"" help:"The source connection string used to extract metadata from the data store"`
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
				return errors.New("configuration/connection string not found")
			}
			return err
		}

		cfg := NewConfig(e.Config)
		err = cfg.Apply(e)
		if err != nil {
			return err
		}
	}

	remote := e.extractor()

	var end_json time.Duration
	var end_sqlite time.Duration
	var end_expand time.Duration

	start_extract := time.Now()

	doc, err := remote.Extract()
	if err != nil {
		return err
	}

	cache.SetDoc(doc)

	end_extract := time.Since(start_extract)
	fmt.Printf("Source Extraction: %s\n", end_extract)

	if len(e.Expand) > 0 {
		start_expand := time.Now()
		remote.ExpandJSONFields(doc, e.SkipViewExpand, e.Expand...)
		end_expand = time.Since(start_expand)
		fmt.Printf("JSON Expansion: %s\n", end_expand)
	}

	switch strings.ToLower(filepath.Ext(e.Outfile)) {
	// JSON
	case ".json":
		start_json := time.Now()
		util.DumpFile(e.Outfile, doc.ToJSON())
		end_json = time.Since(start_json)
		fmt.Printf("Created %s in %s\n", e.Outfile, end_json)
		// case ".xml":
	}

	start_sqlite := time.Now()
	sets := extractor.GetAllSets(doc)
	fmt.Printf("  stashing %v set(s)...\n", len(sets))
	err = cache.UpsertSets("source", sets)
	if err != nil {
		fmt.Println(err)
	}
	items := extractor.GetAllItems(doc)
	fmt.Printf("  stashing %v item(s)...\n", len(items))
	err = cache.UpsertItems("source", items)
	if err != nil {
		fmt.Println(err)
	}
	rels := extractor.GetAllRelationships(doc)
	fmt.Printf("  stashing %v relationship(s)...\n", len(rels))
	err = cache.UpsertRelationships("source", rels)
	if err != nil {
		fmt.Println(err)
	}
	end_sqlite = time.Since(start_sqlite)
	fmt.Printf("Created cache in %s\n", end_sqlite)
	// }

	// if util.InSlice[string]("datahub", e.Extract) {
	fmt.Println("\nNow extracting from Datahub...")
	start_datahub := time.Now()

	dh, err := datahub.New(e.DatahubURL, e.Source, cache, e.APIKey)

	if err == nil {
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
				diff, err := cache.DiffSets()

				if err == nil {
					err = dh.PopulateItems(diff)
					if err == nil {
						items := extractor.GetAllItems(dh.GetDoc())
						fmt.Printf("  stashing %v item(s)...\n", len(items))
						err := cache.UpsertItems("datahub", items)
						if err == nil {
							itemdiff, err := cache.DiffItems(diff)
							if err == nil {
								err = dh.PopulateRelationships(diff)
								if err == nil {
									rels := extractor.GetAllRelationships(dh.GetDoc())
									fmt.Printf("  stashing %v relationship(s)...\n", len(rels))

									err = cache.UpsertRelationships("datahub", rels)
									if err == nil {
										reldiff, err := cache.DiffRelationships(diff)
										if err == nil {
											joindiff, err := cache.DiffJoins(diff, reldiff)
											if err == nil {
												fmt.Printf("\nNow syncing with the Datahub...\n")
												if e.DryRun {
													dh.DryRun(diff, e.Max)
													fmt.Println("")
													dh.DryRun(itemdiff, e.Max, "item")
													fmt.Println("")
													dh.DryRun(reldiff, e.Max, "relationship")
													fmt.Println("")
													dh.DryRun(joindiff, e.Max, "join")
												} else {
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
	} else {
		fmt.Println(err.Error())
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
	}

	return empty
}
