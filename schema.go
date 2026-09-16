package nxsugar

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/jaracil/ei"
	"github.com/xeipuuv/gojsonschema"
)

type Method struct {
	Name   string
	Func   func(*Task) (interface{}, *JsonRpcErr)
	Schema *Schema
	Opts   *MethodOpts
}

type Schema struct {
	FromFile string
	Input    string
	Result   string
	Error    string
	Pacts    []SchemaPact
}

type SchemaPact struct {
	Input  string
	Output string
}

type methodSchema struct {
	source    string
	json      interface{}
	validator *gojsonschema.Schema
}

type methodPact struct {
	input  interface{}
	output interface{}
}

/*
AddSharedSchema adds a schema with an id that can be referenced by method schemas (this schema can be referenced from others with: `{"$ref":"http://nexus.service/id"}`)
*/
func (s *Service) AddSharedSchema(id string, schema string) error {
	return s.addSharedSchema(id, schema)
}

/*
AddSharedSchemaFromFile adds a schema from file with an id that can be referenced by method schemas (this schema can be referenced from others with: `{"$ref":"http://nexus.service/id"}`)
*/
func (s *Service) AddSharedSchemaFromFile(id string, file string) error {
	contents, err := ioutil.ReadFile(file)
	if err != nil {
		err = fmt.Errorf("error adding shared jsonschema (%s) from file (%s): %s", id, file, err.Error())
		s.LogWithFields(ErrorLevel, ei.M{"type": "shared_file", "error": err.Error()}, "error adding shared schema from file")
		return err
	}
	return s.addSharedSchema(id, string(contents))
}

func (s *Service) addSharedSchema(id string, schema string) error {
	if s.sharedSchemas == nil {
		s.sharedSchemas = map[string]gojsonschema.JSONLoader{}
	}
	if _, ok := s.sharedSchemas[id]; ok {
		err := fmt.Errorf("error adding shared jsonschema (%s): an schema with given id already exists", id)
		s.LogWithFields(ErrorLevel, ei.M{"type": "adding_shared", "error": err.Error()}, "error adding shared schema")
		return err
	}
	loader, err := getSchemaLoaderFromJson(schema)
	if err != nil {
		err = fmt.Errorf("error adding shared jsonschema (%s): %s", id, err.Error())
		s.LogWithFields(ErrorLevel, ei.M{"type": "adding_shared", "error": err.Error()}, "error adding shared schema")
		return err
	}
	s.sharedSchemas[id] = loader
	return nil
}

/*
AddMethodSchema adds (or replaces if already added) a method for the service with a JSON schema.
The function that receives the nexus.Task should return a result or an error.
If four arguments are provided, the fourth is a struct containing method options.
If the schema validation does not succeed, an ErrInvalidParams error will be sent as a result for the task.
*/
func (s *Service) AddMethodSchema(name string, schema *Schema, f func(*Task) (interface{}, *JsonRpcErr), opts ...*MethodOpts) error {
	if len(opts) == 0 {
		opts = []*MethodOpts{&MethodOpts{}}
	}
	return s.addMethod(name, schema, f, opts[0])
}

func (s *Service) computeMethodSchemas(name string) {
	// Resolve schemas
	resolvedMethodSchemas := map[string]interface{}{}
	if s.methods[name].inSchema != nil {
		resolvedMethodSchemas["input"] = resolveSchemaRefs(nil, s.methods[name].inSchema.json, s.sharedSchemas)
	}
	if s.methods[name].resSchema != nil {
		resolvedMethodSchemas["result"] = resolveSchemaRefs(nil, s.methods[name].resSchema.json, s.sharedSchemas)
	}
	if s.methods[name].errSchema != nil {
		resolvedMethodSchemas["error"] = resolveSchemaRefs(nil, s.methods[name].errSchema.json, s.sharedSchemas)
	}
	if s.methods[name].pacts != nil && len(s.methods[name].pacts) != 0 {
		resolvedMethodSchemas["pacts"] = resolveSchemaRefs(nil, s.methods[name].pacts, s.sharedSchemas)
	}
	s.methods[name].computedSchema = resolvedMethodSchemas
}

func (s *Service) addSchemaToMethod(name string, schema *Schema) (error, map[string]interface{}) {
	// Add schema from file
	if schema.FromFile != "" {
		file, err := os.Open(schema.FromFile)
		if err != nil {
			return fmt.Errorf("error adding method (%s) jsonschema from file: %s", name, err.Error()), ei.M{"type": "schema_file"}
		}
		var newSch map[string]interface{}
		err = json.NewDecoder(file).Decode(&newSch)
		if err != nil {
			return fmt.Errorf("error adding method (%s) jsonschema from file: %s", name, err.Error()), ei.M{"type": "schema_file"}
		}
		if input, ok := newSch["input"]; ok {
			src, _, validator, err := compileSchema("", input, s.sharedSchemas)
			if err != nil {
				return fmt.Errorf("error adding method (%s) input jsonschema: %s", name, err.Error()), ei.M{"type": "adding_schema"}
			}
			s.methods[name].inSchema = &methodSchema{source: src, json: input, validator: validator}
		}
		if result, ok := newSch["result"]; ok {
			src, _, validator, err := compileSchema("", result, s.sharedSchemas)
			if err != nil {
				return fmt.Errorf("error adding method (%s) result jsonschema: %s", name, err.Error()), ei.M{"type": "adding_schema"}
			}
			s.methods[name].resSchema = &methodSchema{source: src, json: result, validator: validator}
		}
		if errs, ok := newSch["error"]; ok {
			src, _, validator, err := compileSchema("", errs, s.sharedSchemas)
			if err != nil {
				return fmt.Errorf("error adding method (%s) error jsonschema: %s", name, err.Error()), ei.M{"type": "adding_schema"}
			}
			s.methods[name].errSchema = &methodSchema{source: src, json: errs, validator: validator}
		}
		if pacts, ok := newSch["pacts"].([]interface{}); ok {
			for n, pact := range pacts {
				if pactm, ok := pact.(map[string]interface{}); ok {
					if pactInput, ok := pactm["input"]; ok {
						if pactOutput, ok := pactm["output"]; ok {
							s.methods[name].pacts = append(s.methods[name].pacts, &methodPact{
								input:  pactInput,
								output: pactOutput,
							})
						} else {
							return fmt.Errorf("error adding method (%s) pact (%d): missing output field", name, n), ei.M{"type": "adding_pact"}
						}
					} else {
						return fmt.Errorf("error adding method (%s) pact (%d): missing input field", name, n), ei.M{"type": "adding_pact"}
					}
				} else {
					return fmt.Errorf("error adding method (%s) pact (%d): must be map", name, n), ei.M{"type": "adding_pact"}
				}
			}
		}
		file.Close()
		s.computeMethodSchemas(name)
		return nil, nil
	}

	// Add schema from string
	if schema.Input != "" || schema.Result != "" || schema.Error != "" {
		if schema.Input != "" {
			_, sch, validator, err := compileSchemaFromJson(schema.Input, s.sharedSchemas)
			if err != nil {
				return fmt.Errorf("error adding method (%s) input jsonschema: %s", name, err.Error()), ei.M{"type": "adding_schema"}
			}
			s.methods[name].inSchema = &methodSchema{source: schema.Input, json: sch, validator: validator}
		}
		if schema.Result != "" {
			_, sch, validator, err := compileSchemaFromJson(schema.Result, s.sharedSchemas)
			if err != nil {
				return fmt.Errorf("error adding method (%s) result jsonschema: %s", name, err.Error()), ei.M{"type": "adding_schema"}
			}
			s.methods[name].resSchema = &methodSchema{source: schema.Result, json: sch, validator: validator}
		}
		if schema.Error != "" {
			_, sch, validator, err := compileSchemaFromJson(schema.Error, s.sharedSchemas)
			if err != nil {
				return fmt.Errorf("error adding method (%s) error jsonschema: %s", name, err.Error()), ei.M{"type": "adding_schema"}
			}
			s.methods[name].errSchema = &methodSchema{source: schema.Error, json: sch, validator: validator}
		}
		if schema.Pacts != nil {
			for n, pact := range schema.Pacts {
				methPact := &methodPact{}
				err := json.Unmarshal([]byte(pact.Input), &methPact.input)
				if err != nil {
					return fmt.Errorf("error adding method (%s) pact (%d): parsing input json: %s", name, n, err.Error()), ei.M{"type": "adding_pact"}
				}
				err = json.Unmarshal([]byte(pact.Output), &methPact.output)
				if err != nil {
					return fmt.Errorf("error adding method (%s) pact (%d): parsing output json: %s", name, n, err.Error()), ei.M{"type": "adding_pact"}
				}
				s.methods[name].pacts = append(s.methods[name].pacts, methPact)
			}
		}
	}

	s.computeMethodSchemas(name)
	return nil, nil
}

func getSchemaLoaderFromJson(source string) (gojsonschema.JSONLoader, error) {
	var schres interface{}
	err := json.Unmarshal([]byte(source), &schres)
	if err != nil {
		return nil, fmt.Errorf("parsing json: %s", err.Error())
	}
	return getSchemaLoader(schres), nil
}

func getSchemaLoader(d interface{}) gojsonschema.JSONLoader {
	return gojsonschema.NewGoLoader(d)
}

func compileSchemaFromJson(source string, shared map[string]gojsonschema.JSONLoader) (string, interface{}, *gojsonschema.Schema, error) {
	var schres interface{}
	err := json.Unmarshal([]byte(source), &schres)
	if err != nil {
		return "", nil, nil, fmt.Errorf("parsing json: %s", err.Error())
	}
	return compileSchema(source, schres, shared)
}

func compileSchema(source string, d interface{}, shared map[string]gojsonschema.JSONLoader) (string, interface{}, *gojsonschema.Schema, error) {
	schemaLoader := gojsonschema.NewSchemaLoader()
	if shared != nil {
		for id, sch := range shared {
			err := schemaLoader.AddSchema("http://nexus.service/"+id, sch)
			if err != nil {
				return "", nil, nil, fmt.Errorf("invalid: adding shared schema %s: %s", id, err.Error())
			}
		}
	}
	validator, err := schemaLoader.Compile(gojsonschema.NewGoLoader(d))
	if err != nil {
		return "", nil, nil, fmt.Errorf("invalid: %s", err.Error())
	}
	if source == "" {
		sourceb, err := json.Marshal(d)
		if err != nil {
			return "", nil, nil, fmt.Errorf("invalid: %s", err.Error())
		}
		source = string(sourceb)
	}
	return source, d, validator, nil
}

func schemaValidationErr(result *gojsonschema.Result) string {
	if len(result.Errors()) == 1 {
		return fmt.Sprintf("%s", result.Errors()[0])
	} else {
		out := ""
		for _, desc := range result.Errors() {
			out += fmt.Sprintf("\n- %s", desc)
		}
		return out
	}
}

func resolveSchemaRefs(seen map[string]bool, schema interface{}, shared map[string]gojsonschema.JSONLoader) interface{} {
	schMap, errMap := ei.N(schema).MapStr()
	if errMap != nil {
		return schema
	}

	if seen == nil {
		seen = map[string]bool{}
	}

	// If a valid $ref is found return it
	for f, v := range schMap {
		if f == "$ref" {
			refVal := ei.N(v).StringZ()
			if !seen[refVal] {
				seen[refVal] = true
				id, pointer := schemaRefParse(refVal)
				if id != "" {
					if found := findSharedSchemaRef(id, pointer, shared); found != nil {
						return resolveSchemaRefs(copySeenMap(seen), found, shared)
					}
				}
			}
			return schema
		}
	}

	// If no valid $ref is found, resolve the fields
	newMap := map[string]interface{}{}
	for f, v := range schMap {
		if _, err := ei.N(v).MapStr(); err == nil {
			newMap[f] = resolveSchemaRefs(copySeenMap(seen), v, shared)
		} else if arrV, err := ei.N(v).Slice(); err == nil {
			newArrV := []interface{}{}
			for _, av := range arrV {
				newArrV = append(newArrV, resolveSchemaRefs(copySeenMap(seen), av, shared))
			}
			newMap[f] = newArrV
		} else {
			newMap[f] = v
		}
	}

	return newMap
}

func copySeenMap(seen map[string]bool) map[string]bool {
	newSeen := map[string]bool{}
	for f, v := range seen {
		newSeen[f] = v
	}
	return newSeen
}

func findSharedSchemaRef(id string, pointer string, shared map[string]gojsonschema.JSONLoader) interface{} {
	if s, ok := shared[id]; ok {
		sch := s.JsonSource()
		if pointer != "" {
			var err error
			for _, p := range getJsonPointerList(pointer) {
				sch, err = ei.N(sch).M(p).Raw()
				if err != nil {
					return nil
				}
			}
		}
		return sch
	}
	return nil
}

func schemaRefParse(ref string) (string, string) {
	var id, pointer string
	if strings.HasPrefix(ref, "http://nexus.service/") {
		ref = strings.TrimPrefix(ref, "http://nexus.service/")
		spl := strings.SplitN(ref, "#", 2)
		if len(spl) != 0 {
			id = spl[0]
			if len(spl) == 2 {
				pointer = spl[1]
			}
		}
	}
	return id, pointer
}

func getJsonPointerList(pointer string) []string {
	pointer = strings.TrimPrefix(pointer, "/")
	spl := strings.Split(pointer, "/")
	if len(spl) == 0 {
		return []string{}
	}
	return spl
}

func schemaPathAdd(path string, add string) string {
	if path == "" {
		return add
	}
	return path + "." + add
}
