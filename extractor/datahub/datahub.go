package datahub

import (
	"bytes"
	"dhs/archive"
	"dhs/extractor/doc"
	"dhs/util"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

type Datahub struct {
	root           string
	token          string
	reattemptlogin bool
	doc            *doc.Doc
	source         string
	sourcedata     map[string]interface{}
	archive        *archive.Archive
}

func New(root string, datasource string, a *archive.Archive, apikey ...string) (*Datahub, error) {
	root = util.EncodeURL(root, apikey...)

	_, err := url.Parse(root)
	if err != nil {
		return &Datahub{}, err
	}

	var api_key string
	if len(apikey) > 0 {
		api_key = apikey[0]
	}

	return &Datahub{
		root:           root,
		reattemptlogin: true,
		token:          api_key,
		// doc: a.Doc(),
		doc: doc.New(&doc.Source{
			Name: doc.Name{Physical: datasource},
		}),
		source:     datasource,
		sourcedata: map[string]interface{}{},
		archive:    a,
	}, nil
}

func (dh *Datahub) SetDoc(d *doc.Doc) {
	dh.doc = d
}

func (dh *Datahub) GetDoc() *doc.Doc {
	return dh.doc
}

func (dh *Datahub) PopulateSources() error {
	id := dh.source
	cd, body, err := dh.get("/catalog/source/" + id + "?expand=sets")
	if err != nil {
		return err
	}

	if cd != 200 {
		if cd == 404 {
			cd, body, err = dh.get("/catalog/sources")
			if err != nil {
				return err
			}

			if cd != 200 {
				return errors.New(fmt.Sprintf("failed to retrieve list of data sources (HTTP %v)", cd))
			}

			var d map[string]interface{}
			err = json.Unmarshal(body, &d)
			if err != nil {
				return err
			}

			if sources, exist := d["sources"]; exist {
				for _, source := range sources.([]interface{}) {
					src := source.(map[string]interface{})
					if src["id"] != nil && src["id"].(string) != dh.source {
						dh.source = src["id"].(string)
						dh.doc.Source().Name.Physical = src["name"].(map[string]interface{})["physical"].(string)
						dh.doc.Source().Name.Logical = src["name"].(map[string]interface{})["logical"].(string)
						return dh.PopulateSources()
					}
				}
			}

			return nil
		} else {
			fmt.Printf("HTTP response status code %v\n", cd)
			return errors.New("failed to return " + id + " data source.")
		}
	}

	var data map[string]interface{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		return err
	}

	dh.sourcedata = data

	src := dh.doc.Source()
	src.Name = doc.Name{
		Logical:  data["name"].(map[string]interface{})["logical"].(string),
		Physical: data["name"].(map[string]interface{})["physical"].(string),
	}

	schema := dh.doc.ApplySchema(&doc.Schema{
		Name:          doc.Name{Physical: src.Name.Physical},
		Comment:       data["description"].(string),
		Metadata:      data["metadata"].(map[string]interface{}),
		Relationships: make(map[string]*doc.Relationship),
		Sets:          make(map[string]*doc.Set),
	})

	if data["sets"] != nil {
		for _, item := range data["sets"].([]interface{}) {
			set := item.(map[string]interface{})
			s := schema.UpsertSet(&doc.Set{
				Id: set["id"].(string),
				Name: doc.Name{
					Physical: set["name"].(map[string]interface{})["physical"].(string),
					Logical:  set["name"].(map[string]interface{})["logical"].(string),
				},
				Comment: set["description"].(string),
				FQDN:    set["stub"].(string),
			})

			if set["definition"] != nil && len(strings.TrimSpace(set["definition"].(string))) > 0 {
				s.Source = set["definition"].(string)
			}

			if set["metadata"] != nil {
				s.Metadata = set["metadata"].(map[string]interface{})

				if src, exists := set["metadata"].(map[string]interface{})["view_source"]; exists {
					s.Source = src.(string)
				}
			}
		}
	}

	// util.Dump(data)

	// fmt.Println(string(dh.doc.ToJSON()))

	return nil
}

func (dh *Datahub) PopulateItems(diff *archive.Diff) error {
	id := dh.source
	uri := "/catalog/source/" + id + "/sets"
	cd, body, err := dh.get(uri)
	if err != nil {
		return err
	}

	if cd != 200 {
		if cd == 404 {
			uri = "/catalog/source/" + id + "/sets?expand=items"
			cd, body, err = dh.get(uri)
			if err != nil {
				return err
			}

			if cd != 200 {
				return errors.New(fmt.Sprintf("failed to retrieve list of data items (HTTP %v) for GET %v\n", cd, uri))
			}

			var d map[string]interface{}
			err = json.Unmarshal(body, &d)
			if err != nil {
				return err
			}

			if items, exist := d["items"]; exist {
				for _, item := range items.([]interface{}) {
					i := item.(map[string]interface{})
					fmt.Println(i)
					// if i["id"] != nil && i["id"].(string) != dh.item {
					// 	// dh.source = src["id"].(string)
					// 	// dh.doc.Source().Name.Physical = src["name"].(map[string]interface{})["physical"].(string)
					// 	// dh.doc.Source().Name.Logical = src["name"].(map[string]interface{})["logical"].(string)
					// 	// return dh.PopulateSources()
					// }
				}
			}

			return nil
		} else {
			fmt.Printf("HTTP response status code %v for GET %v\n", cd, uri)
			return errors.New("failed to return " + id + " data set.")
		}
	}

	var data map[string]interface{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		return err
	}

	// util.DumpFile("./tmp.json", data)

	src := dh.doc.Source()
	for _, raw := range data["sets"].([]interface{}) {
		record := raw.(map[string]interface{})

		schema := dh.doc.ApplySchema(&doc.Schema{
			Name: doc.Name{Physical: src.Name.Physical},
		})

		set, err := schema.GetSet(record["name"].(map[string]interface{})["physical"].(string))
		if err != nil {
			return err
		}

		for _, itemdata := range record["items"].([]interface{}) {
			i := itemdata.(map[string]interface{})
			item := set.UpsertItem(&doc.Item{
				Id: i["id"].(string),
				Name: doc.Name{
					Logical:  i["name"].(map[string]interface{})["logical"].(string),
					Physical: i["name"].(map[string]interface{})["physical"].(string),
				},
				Comment: i["description"].(string),
				// Type: getType(),
				Type: i["type"].(string),
				// UDTType:  i["type"].(string),
				Nullable: i["nullable"].(bool),
				FQDN:     i["stub"].(string),
				// Example: i["example"].(string),
			})

			if i["default"] != nil {
				item.Default = i["default"].(string)
			}
			if i["example"] != nil {
				item.Example = i["example"].(string)
			}
			if i["metadata"] != nil {
				item.Metadata = i["metadata"].(map[string]interface{})
			}
			if i["key"] != nil {
				keys := i["key"].([]interface{})
				// (map[string]interface{})

				for _, k := range keys {
					if k.(map[string]interface{})["is_key"].(bool) {
						var t string
						if k.(map[string]interface{})["primary"].(bool) {
							t = "primary"
						} else {
							t = "foreign"
						}

						item.UpsertKey(&doc.Key{
							Name:  k.(map[string]interface{})["name"].(string),
							Type:  t,
							Items: []string{item.Name.Physical},
						})
					}
				}
			}
		}
	}

	return nil
}

func (dh *Datahub) PopulateRelationships(diff *archive.Diff) error {
	id := dh.source
	uri := "/catalog/relationships/source/" + id
	cd, body, err := dh.get(uri)
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

	src := dh.doc.Source()

	schema := dh.doc.ApplySchema(&doc.Schema{
		Name: doc.Name{Physical: src.Name.Physical},
	})

	for _, relationship := range data["relationships"].([]interface{}) {
		raw := relationship.(map[string]interface{})

		if raw["items"].([]interface{})[0].(map[string]interface{})["parent"] != nil {
			set, err := schema.GetSet(raw["items"].([]interface{})[0].(map[string]interface{})["parent"].(map[string]interface{})["set"].(map[string]interface{})["name"].(map[string]interface{})["physical"].(string))
			if err != nil {
				fmt.Println(err)
			}

			rel := schema.UpsertRelationship(&doc.Relationship{
				Id: raw["id"].(string),
				Name: doc.Name{
					Physical: raw["name"].(map[string]interface{})["physical"].(string),
					Logical:  raw["name"].(map[string]interface{})["logical"].(string),
				},
				// Type: raw["match_type"].(string),
				Comment: raw["description"].(string),
				Integrity: &doc.ReferentialIntegrity{
					Update: raw["referential_integrity"].(map[string]interface{})["on_update"].(string),
					Delete: raw["referential_integrity"].(map[string]interface{})["on_delete"].(string),
					Match:  raw["match_type"].(string),
				},
				Items: make([]*doc.Join, 0),
				Set:   set,
			})

			for _, item := range raw["items"].([]interface{}) {
				i := item.(map[string]interface{})

				parent := i["parent"].(map[string]interface{})
				child := i["child"].(map[string]interface{})

				cardinality := []string{}
				for _, rule := range raw["cardinality"].(map[string]interface{})["raw"].([]interface{}) {
					str := strconv.Itoa(int(rule.(float64)))
					cardinality = append(cardinality, str)
				}

				rel.Items = append(rel.Items, &doc.Join{
					Parent: &doc.RelItem{
						Schema: parent["set"].(map[string]interface{})["source"].(map[string]interface{})["stub"].(string),
						Set:    parent["set"].(map[string]interface{})["name"].(map[string]interface{})["physical"].(string),
						Item:   parent["name"].(map[string]interface{})["physical"].(string),
						FQDN:   parent["stub"].(string),
					},
					Child: &doc.RelItem{
						Schema: child["set"].(map[string]interface{})["source"].(map[string]interface{})["stub"].(string),
						Set:    child["set"].(map[string]interface{})["name"].(map[string]interface{})["physical"].(string),
						Item:   child["name"].(map[string]interface{})["physical"].(string),
						FQDN:   child["stub"].(string),
					},
					Cardinality:  strings.Join(cardinality, ","),
					Relationship: rel,
				})
			}
		}
		// util.Dump(raw)
	}

	return nil
}

func (dh *Datahub) getAuthToken() error {
	if dh.token != util.EmptyString {
		return nil
	}

	fmt.Println("  authenticating with Datahub service...")
	cd, body, err := dh.get("/token")
	if err != nil {
		return err
	}

	if cd != 200 {
		fmt.Printf("access denied (response code %v)\n", cd)
		return errors.New("access denied")
	}

	var data map[string]interface{}
	err = json.Unmarshal(body, &data)
	if err != nil {
		return err
	}

	dh.token = data["jwt"].(string)
	dh.reattemptlogin = true

	return nil
}

func (dh *Datahub) get(endpoint string) (int, []byte, error) {
	uri, err := url.Parse(dh.root + endpoint)
	if err != nil {
		return 0, util.EmptyByte, err
	}

	req, err := http.NewRequest("GET", uri.String(), nil)
	if err != nil {
		return 0, []byte{}, err
	}

	if dh.token != util.EmptyString {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", dh.token))
	}

	client := &http.Client{}

	res, err := client.Do(req)
	if err != nil {
		return 0, util.EmptyByte, err
	}
	defer res.Body.Close()

	if res.StatusCode == 401 && dh.reattemptlogin {
		dh.reattemptlogin = false
		dh.token = util.EmptyString

		err = dh.getAuthToken()
		if err == nil {
			dh.reattemptlogin = true
			return dh.get(endpoint)
		}
	}

	// fmt.Print("GET %v%v%v\n", dh.root, endpoint)

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return 0, util.EmptyByte, err
	}

	return res.StatusCode, body, err
}

func (dh *Datahub) send(method string, endpoint string, data interface{}) (int, interface{}, error) {
	fmt.Printf("  HTTP %v %v\n", method, endpoint)

	if method == "POST" {
		util.DumpLog("./post.log", map[string]interface{}{
			"endpoint": endpoint,
			"data":     data,
		})
	}

	body, _ := json.Marshal(data)
	var res interface{}

	uri, err := url.Parse(dh.root + endpoint)
	if err != nil {
		return 0, res, err
	}

	req, err := http.NewRequest(method, uri.String(), bytes.NewBuffer(body))
	if err != nil {
		return 0, res, err
	}

	if dh.token != util.EmptyString {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", dh.token))
	}

	req.Header.Set("Content-Type", "application/json")

	client := http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return 0, res, err
	}
	defer response.Body.Close()

	if response.StatusCode == 401 && dh.reattemptlogin {
		dh.reattemptlogin = false
		dh.token = util.EmptyString

		err = dh.getAuthToken()
		if err == nil {
			dh.reattemptlogin = true
			return dh.send(method, endpoint, data)
		}
	}

	content, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return 0, res, err
	}

	if response.StatusCode != 200 && response.StatusCode != 201 {
		return response.StatusCode, res, errors.New(fmt.Sprintf("request failure (%v): %s\n", response.StatusCode, content))
	}

	// fmt.Printf("%s\n%v", content, response.StatusCode)

	var resbody interface{}
	err = json.Unmarshal(content, &resbody)
	if err != nil {
		if response.StatusCode == 200 || response.StatusCode == 201 {
			var x interface{}
			return response.StatusCode, x, nil
		}

		return response.StatusCode, map[string]interface{}{"raw": string(content)}, err
	}

	return response.StatusCode, resbody, nil
}

func (dh *Datahub) post(endpoint string, data interface{}) (int, interface{}, error) {
	return dh.send("POST", endpoint, data)
}

func (dh *Datahub) put(endpoint string, data interface{}) (int, interface{}, error) {
	return dh.send("PUT", endpoint, data)
}

func (dh *Datahub) delete(endpoint string, data ...interface{}) (int, interface{}, error) {
	fmt.Printf("  HTTP DELETE %v\n", endpoint)
	var res interface{}

	uri, err := url.Parse(dh.root + endpoint)
	if err != nil {
		return 0, res, err
	}

	var req *http.Request
	if len(data) > 0 {
		body, _ := json.Marshal(data[0])
		req, err = http.NewRequest("DELETE", uri.String(), bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req, err = http.NewRequest("DELETE", uri.String(), nil)
	}
	if err != nil {
		return 0, res, err
	}

	if dh.token != util.EmptyString {
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", dh.token))
	}

	client := http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return 0, res, err
	}
	defer response.Body.Close()

	if response.StatusCode == 401 && dh.reattemptlogin {
		dh.reattemptlogin = false
		dh.token = util.EmptyString

		err = dh.getAuthToken()
		if err == nil {
			dh.reattemptlogin = true
			return dh.delete(endpoint, data...)
		}
	}

	// if len(data) > 0 {
	// 	body, _ := json.MarshalIndent(data, "", "  ")
	// 	fmt.Println(string(body))
	// }
	content, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return 0, res, err
	}

	var resbody interface{}
	err = json.Unmarshal(content, &resbody)

	return response.StatusCode, resbody, nil
}

func (dh *Datahub) DryRun(d *archive.Diff, max int, datatype ...string) {
	data := "set"
	if len(datatype) > 0 {
		data = datatype[0]
	}

	if len(d.Added) > 0 {
		fmt.Printf("  %v %v(s) will be added to the Datahub\n", len(d.Added), data)
		i := 0
		for _, el := range d.Added {
			i++
			if i <= max {
				if data == "set" {
					fmt.Printf("    + %v\n", el.(*doc.Set).Name.Physical)
				} else if data == "item" {
					i := el.(*doc.Item)
					fmt.Printf("    + %v.%v\n", i.Set().Name.Physical, i.Name.Physical)
				} else if data == "join" {
					i := el.(*doc.Join)
					fmt.Printf("    + %v -> %v\n", i.Parent.FQDN, i.Child.FQDN)
				} else {
					i := el.(*doc.Relationship)
					fmt.Printf("    + %v\n", i.Name.Physical)
				}
			} else if i == (max + 1) {
				fmt.Printf("    + and more...\n")
				break
			}
		}
	}

	if len(d.Deleted) > 0 {
		fmt.Printf("\n  %v %v(s) will be removed from the Datahub\n", len(d.Deleted), data)
		i := 0
		for _, el := range d.Deleted {
			i++
			if i <= max {
				if data == "set" {
					s := el.(*doc.Set)
					fmt.Printf("    - %v (%v)\n", s.Name.Physical, s.Id)
				} else if data == "item" {
					s := el.(*doc.Item)
					fmt.Printf("    - %v.%v (%v)\n", s.Set().Name.Physical, s.Name.Physical, s.Id)
				} else if data == "join" {
					s := el.(*doc.Join)
					fmt.Printf("    + %v -> %v\n", s.Parent.FQDN, s.Child.FQDN)
				} else {
					s := el.(*doc.Relationship)
					fmt.Printf("    - %v (%v)\n", s.Name.Physical, s.Id)
				}
			} else if i == (max + 1) {
				fmt.Printf("    - and more...\n")
				break
			}
		}
	}

	if len(d.Updated) > 0 {
		fmt.Printf("\n  %v %v(s) will be updated in the Datahub\n", len(d.Updated), data)
		i := 0
		for _, el := range d.Updated {
			i++
			if i <= max {
				if data == "set" {
					fmt.Printf("    ! %v (%v)\n", el.(*doc.Set).Name.Physical, el.(*doc.Set).Id)
				} else if data == "item" {
					fmt.Printf("    ! %v.%v (%v)\n", el.(*doc.Item).Set().Name.Physical, el.(*doc.Item).Name.Physical, el.(*doc.Item).Id)
				} else if data == "join" {
					fmt.Printf("    + %v -> %v\n", el.(*doc.Join).Parent.FQDN, el.(*doc.Join).Child.FQDN)
				} else {
					fmt.Printf("    ! %v (%v)\n", el.(*doc.Relationship).Name.Physical, el.(*doc.Relationship).Id)
				}
			} else if i == (max + 1) {
				fmt.Printf("    ! and more...\n")
				break
			}
		}
	}
}

func (dh *Datahub) Commit(diffs ...*archive.Diff) error {
	for _, d := range diffs {
		// Deletions
		rels := []string{}
		if len(d.Deleted) > 0 {
			fmt.Println("\n  committing deletions...")
			var status int
			var err error

			for _, obj := range d.Deleted {
				switch value := obj.(type) {
				case *doc.Set:
					status, _, err = dh.delete("/catalog/set/" + value.Id)
					if err != nil || status != 200 {
						fmt.Printf("Error deleting set %v (HTTP %v)\n%v\n", value.Id, status, err)
					}
				case *doc.Item:
					status, _, err = dh.delete("/catalog/item/" + value.Id)
					if err != nil || status != 200 {
						fmt.Printf("Error deleting item %v (HTTP %v)\n%v\n", value.Id, status, err)
					}
				case *doc.Relationship:
					rels = append(rels, value.Id)
				}
			}

			if len(rels) > 0 {
				status, b, err := dh.delete("/catalog/relationships", map[string]interface{}{
					"relationships": rels,
				})

				// util.Dump(map[string]interface{}{
				// 	"relationships": rels,
				// })

				if err != nil || status != 200 {
					fmt.Printf("Error deleting relationships (HTTP %v)\n", status)
					fmt.Println(b)
				}
			}
			// } else {
			// 	fmt.Println("\n  skipping deletions (none detected)")
		}

		// Additions
		if len(d.Added) > 0 {
			fmt.Println("\n  committing additions...")
			items := make(map[string][]interface{})
			rels := make([]map[string]interface{}, 0)
			for _, obj := range d.Added {
				switch value := obj.(type) {
				case *doc.Set:
					// The bulk endpoint is not used because it does not return the new ID for each set.
					// The new ID is required to add or **update** items and relationships.
					status, result, err := dh.post("/catalog/source/"+dh.sourcedata["id"].(string)+"/set", value.ToPostBody())
					if status != 201 && err == nil {
						fmt.Println(result)
					}
					if err != nil {
						fmt.Printf("error creating %v set: %v\n", value.Name.Physical, err.Error())
					} else {
						value.Id = result.(map[string]interface{})["id"].(string)
					}
				case *doc.Item:
					id := value.Set().Id
					if id == util.EmptyString {
						s, err := value.Set().GetSchemaObject().GetSet(value.Set().Name.Physical)
						if err != nil {
							fmt.Println(err)
						}

						if s.Id == util.EmptyString {
							tmpset, err := dh.LookupSet(value.Set().Name.Physical, dh.sourcedata["id"].(string))
							if err == nil {
								id = tmpset.Id
							} else {
								fmt.Println(err)
								break
							}
						} else {
							id = s.Id
						}
					}

					if items[id] == nil {
						items[id] = make([]interface{}, 0)
					}
					items[id] = append(items[id], value.ToPostBody())
				case *doc.Relationship:
					rels = append(rels, value.ToPostBody())
				}
			}

			if len(items) > 0 {
				for id, body := range items {
					if id == util.EmptyString || len(strings.TrimSpace(id)) == 0 {
						fmt.Println("Failed to add item (no set associated with item):")
						util.Dump(body)
					} else {
						dh.post("/catalog/set/"+id+"/items", map[string]interface{}{
							"items": body,
						})
					}
				}
			}

			if len(rels) > 0 {
				status, _, err := dh.post("/catalog/relationships", map[string]interface{}{
					"relationships": rels,
				})

				if err != nil {
					fmt.Printf("error creating relationships (HTTP %v): %v\n", status, err.Error())
				}
			}
			// } else {
			// 	fmt.Println("\n  skipping additions (none detected)")
		}

		// Updates
		if len(d.Updated) > 0 {
			fmt.Println("\n  committing updates...")
			sets := make(map[string][]map[string]interface{})
			rels := make([]map[string]interface{}, 0)
			for _, obj := range d.Updated {
				switch value := obj.(type) {
				case *doc.Set:
					data := value.ToPostBody()
					delete(data, "items")
					dh.put("/catalog/set/"+value.Id, data)
					// util.Dump(data)
				case *doc.Item:
					if sets[value.Set().Id] == nil {
						sets[value.Set().Id] = make([]map[string]interface{}, 0)
					}
					sets[value.Set().Id] = append(sets[value.Set().Id], value.ToPostBody())
				case *doc.Relationship:
					rels = append(rels, value.ToPostBody())
				}
			}

			if len(sets) > 0 {
				for id, data := range sets {
					// if false {
					// 	fmt.Println(id)
					// }
					// util.Dump(map[string]interface{}{
					// 	"items": data,
					// })
					status, _, err := dh.post("/catalog/set/"+id+"/items", map[string]interface{}{
						"items": data,
					})
					if err != nil {
						fmt.Println(err)
					} else if status != 201 && status != 200 {
						fmt.Printf("%v set item updates failed with HTTP %v\n", id, status)
					}
				}
				// util.DumpFile("tmp.json", sets)
			}

			if len(rels) > 0 {
				status, _, err := dh.put("/catalog/relationships", map[string]interface{}{
					"relationships": rels,
				})

				if err != nil {
					fmt.Println(err)
				} else if status != 201 && status != 200 {
					fmt.Printf("%v relationship updates failed with HTTP %v\n", len(rels), status)
				}

				// util.Dump(body)
			}
			// } else {
			// 	fmt.Println("\n  skipping updates (none detected)")
		}
	}

	return nil
}

func (dh *Datahub) LookupSet(name string, schema string) (*doc.Set, error) {
	id := ""
	rs, err := dh.archive.LookupDatahubSet(name)
	if err == nil {
		if rs.Count() > 0 {
			id = rs.Get(0)["id"].(string)
		} else {
			status, result, err := dh.get("/catalog/schema/" + schema + "/sets")
			if err == nil {
				if status == 200 {
					var res map[string]interface{}
					json.Unmarshal(result, &res)
					for _, set := range res["sets"].([]map[string]interface{}) {
						if strings.ToLower(strings.TrimSpace(set["name"].(map[string]interface{})["physical"].(string))) == strings.ToLower(strings.TrimSpace(name)) {
							return &doc.Set{Id: set["id"].(string)}, nil
						}
					}
					err = errors.New("schema does not contain \"" + name + "\" set")
				} else {
					err = errors.New(fmt.Sprintf("not found (in Datahub - HTTP %v)", status))
				}
			}
		}
	}

	return &doc.Set{Id: id}, err
}
