package main

import "io"
import "time"
import "sync"
import "flag"
import "strconv"
import "net/http"
import "fmt"
import "io/ioutil"
import "database/sql"
import "encoding/json"
import "github.com/pkg/errors"
import "github.com/golang/glog"
import "github.com/gocraft/web"
import _ "github.com/lib/pq"


var (
	db    *sql.DB
	lock  sync.RWMutex
	cache = make(map[string]string, 100)
)

const (
    DB_USER = "postgres"
    DB_NAME = "mypgdb"
)

func main() {
	var err error

	byteArray, err := ioutil.ReadFile("./db/db_info.txt")
	if err != nil {
		glog.Fatal(err)
	}
	DB_PASSWORD := fmt.Sprintf("%s", byteArray)
	
	connStr := fmt.Sprintf("postgres://%s:%s@127.0.0.1:5433/%s?sslmode=disable",
            DB_USER, DB_PASSWORD, DB_NAME)

	flag.Set("logtostderr", "true")
	flag.Set("v", "2")
	flag.Parse()

	router := web.New(Context{})
	router.Middleware((*Context).Logs)
	router.Middleware((*Context).Errors)
	router.Get("/docs", (*Context).Docs)
	router.Get("/docs/:doc_id", (*Context).Doc)
	router.Get("/dashboard", (*Context).RenderUpPage)
	router.Delete("/docs/:doc_id", (*Context).DeleteDoc)
	
	router.Post("/upload", (*Context).UpDoc)

	if db, err = sql.Open("postgres", connStr); err != nil {
		glog.Fatal(err)
	}

	if err = db.Ping(); err != nil {
		glog.Fatal(err)
	}

	if err = http.ListenAndServe(":8080", router); err != nil {
		glog.Fatal(err)
	}
}


type Doc struct {
	ID   	int 	 `json:"id"`
	Name 	string 	 `json:"name"`
	Mime    string   `json:"mime"`
	Public  string   `json:"public"`
	File    string   `json:"file"`
	Created string   `json:"created"`
}


type Docs struct {
	data *[]Doc `json:"fetched_data"`
}


type Context struct {
	err error
}


func (c *Context) GetDoc(id string) string {
	lock.RLock()
	defer lock.RUnlock()
	return cache[id]
}

func (c *Context) SetDoc(id, text string) {
	lock.Lock()
	defer lock.Unlock()
	cache[id] = text
}

func (c *Context) ReadDoc(id string) string {
	var err error
	var intID int
	var data []byte

	intID, err = strconv.Atoi(id)
	if err != nil {
		c.err = errors.Wrap(err, "converting doc ID")
		return ""
	}

	doc := c.SelectDoc(intID)
	if c.err != nil {
		return ""
	}
	if doc == nil {
		return ""
	}

	data, err = json.Marshal(doc)
	if err != nil {
		c.err = errors.Wrap(err, "marshaling doc")
		return ""
	}

	return string(data)
}


func (c *Context) Errors(rw web.ResponseWriter, req *web.Request, next web.NextMiddlewareFunc) {

	next(rw, req)

	if c.err != nil {
		glog.Errorf("Ошибка: %+v", c.err)
		rw.WriteHeader(http.StatusInternalServerError)
	}
}


func (c *Context) Logs(rw web.ResponseWriter, req *web.Request, next web.NextMiddlewareFunc) {

	start := time.Now()

	next(rw, req)

	glog.Infof("[ %s ][ %s ] %s", time.Since(start), req.Method, req.URL)
}


func (c *Context) Docs(rw web.ResponseWriter, req *web.Request) {
	var err error
	var data []byte

	docs := c.Select()
	if c.err != nil {
		return
	}

	data, err = json.Marshal(docs)
	if err != nil {
		c.err = errors.Wrap(err, "marshaling docs")
		return
	}

	rw.Header().Set("Content-Type", "application/json")
	rw.Write(data)
}

func (c *Context) Doc(rw web.ResponseWriter, req *web.Request) {
	var text string

	id := req.PathParams["doc_id"]

	args := req.URL.Query()
	if args.Get("force") != "1" {
		text = c.GetDoc(id)
	}

	if text == "" {
		glog.Info("cache miss")
		text = c.ReadDoc(id)
		if c.err != nil {
			return
		}
		c.SetDoc(id, text)
	}

	if text == "" {
		rw.WriteHeader(http.StatusNotFound)
		return
	}

	rw.Header().Set("Content-Type", "application/json")
	io.WriteString(rw, text)
}

func (c *Context) Select() (docs []*Doc) {

	docs = make([]*Doc, 0, 100)

	rows, err := db.Query("select id, name, mime, file, public, created from docs;")
	if err != nil {
		c.err = errors.Wrap(err, "selecting docs")
		return
	}
	defer rows.Close()

	for rows.Next() {

		doc := new(Doc)
		err = rows.Scan(&doc.ID, &doc.Name, &doc.Mime, &doc.File, &doc.Public, &doc.Created)
		if err != nil {
			c.err = errors.Wrap(err, "scanning docs")
			return
		}

		docs = append(docs, doc)

	}

	if err = rows.Err(); err != nil {
		c.err = errors.Wrap(err, "finalizing doc")
		return
	}

	return
}


func (c *Context) SelectDoc(id int) (doc *Doc) {

	rows, err := db.Query(`
	select id, name, mime, file, public, created
	from docs
	where 
	id = $1;`, id)
	if err != nil {
		c.err = errors.Wrap(err, "selecting doc")
		return
	}
	defer rows.Close()

	if rows.Next() {

		doc = new(Doc)
		err = rows.Scan(&doc.ID, &doc.Name, &doc.Mime, &doc.File, &doc.Public, &doc.Created)
		if err != nil {
			c.err = errors.Wrap(err, "scanning doc")
			return
		}

	}

	if err = rows.Err(); err != nil {
		c.err = errors.Wrap(err, "finalizing doc")
		return
	}

	return
}

func (c *Context) RenderUpPage(rw web.ResponseWriter, req *web.Request) {
	http.ServeFile(rw, req.Request, "views/upload.html")
	return
}


func (c *Context) UpDoc(rw web.ResponseWriter, req *web.Request) {
	req.ParseMultipartForm(32 << 20)
	file, header, err := req.FormFile("uploadfile")
	if err != nil {
	   c.err = errors.Wrap(err, "parsing FormFile")
	   return
	}
	defer file.Close()

	
	query := `insert into docs (name, mime, file, public)
		values ($1, $2, $3, $4)` 
	mime := header.Header.Get("Content-Type")
	_, err = db.Exec(query, header.Filename, mime, true, true)
	if err != nil {
		c.err = errors.Wrap(err, "trying to insert into docs")
		return
	}

	
	var idScanned sql.NullString
	err = db.QueryRow("select id from docs where name = $1 and mime = $2", header.Filename, mime).Scan(&idScanned)
	if err != nil || !(idScanned.Valid) {
		c.err = errors.Wrap(err, "what a trouble")
		return
	}

	c.SetDoc(idScanned.String, "")

	
	return
}

func (c *Context) DeleteDoc(rw web.ResponseWriter, req *web.Request) {
	id, err := strconv.Atoi(req.PathParams["doc_id"])
	if err != nil {
		c.err = errors.Wrap(err, "detecting document id")
		return
	}

	c.SetDoc(req.PathParams["doc_id"], "")
	

	query := `delete from docs where id = $1;`
	_, err = db.Exec(query, id)
	if err != nil {
		c.err = errors.Wrap(err, "deleting item from docs")
		return
	}

	return
}
