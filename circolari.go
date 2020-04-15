// package main contains the Main function where everything happens.
// The following ENV variables are required.
// CIRCULARS_DB_CONNECTION_STRING=db_user:db_pass@tcp(db_host:db_port)/db_name
// CIRCULARS_SITE_URL=https://web.spaggiari.eu/sdg/app/default/comunicati.php?sede_codice=XXXX0000
// CIRCULARS_CYCLE_WAIT=5m
package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/PuerkitoBio/goquery"
	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/net/html"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// type moreCircularsMsg is used for parsing the response after asking if there are more circulars to be loaded.
// This is required since the server only send 100 circulars at a time
type moreCircularsMsg struct {
	Status bool
	Data   int
	Err    string
	Errdbg string
	// Htm = table lines with circulars
	Htm string
	// Cnt = Number of circulars available in next request
	Cnt int
}

type attachment struct {
	Id    uint64
	Title string
}

type circular struct {
	// Id = attr 'id_doc` of tag with class 'download-file'
	Id             uint64
	Title          string
	Category       string
	PublishedDate  time.Time
	ValidUntilDate time.Time
	// Attachments = array of 'id_doc' from tags with class 'link-to-file'
	Attachments []attachment
}

type dbConfig struct {
	ConnectionString string
}

// getCirculars returns all the circulars from the "segreteria digitale" of your school as parsable html.
// siteUrl -> "https://web.spaggiari.eu/sdg/app/default/comunicati.php?sede_codice=XXXX0000"
func getCirculars(siteUrl string) (*strings.Reader, error) {
	client := &http.Client{}
	count := 0
	circularsHtml := ""

	// get circulars 100 per request
	for {
		req, err := http.NewRequest("POST", siteUrl, strings.NewReader(url.Values{"a": {"akSEARCH"}, "field": {"default"}, "search_term": {""}, "visua_storico": {"false"}, "ls": {strconv.Itoa(count)}}.Encode()))
		if err != nil {
			return nil, err
		}
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Add("X-Requested-With", "XMLHttpRequest")
		req.Header.Add("Accept-Charset", "UTF-8")
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}

		var m moreCircularsMsg
		if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
			return nil, errors.New("can't parse response body")
		}
		resp.Body.Close()

		circularsHtml += m.Htm
		if m.Cnt <= 0 {
			break
		}
		count += 100
	}

	return strings.NewReader("<html><body><table>" + circularsHtml + "</table></body></html>"), nil
}

// findNodeWithContext search the first node where the previous sibling Data contains the substring passed in as context.
// In case node is nil, use 'exists' to check whether the node was found or not
func findNodeWithContext(context string, s []*html.Node) (node *html.Node, exists bool) {
	for _, n := range s {
		if prev := n.PrevSibling.Data; strings.Contains(prev, context) {
			return n.FirstChild, true
		}
	}
	return nil, false
}

// function parseCirculars parses the html structure that's received
func parseCirculars(circularsHtml *strings.Reader) (circulars []circular, err error) {
	numRowResult := 0

	// Load the HTML doc
	doc, err := goquery.NewDocumentFromReader(circularsHtml)
	if err != nil {
		return nil, err
	}

	// Parse single circular
	doc.Find("tr.row-result").Each(func(i int, row *goquery.Selection) {
		// Parse circular ID
		var id uint64
		idStr, exist := row.Find(".download-file").Attr("id_doc")
		if !exist {
			return
		}
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			log.Println("ERROR: can't parse id to int. Skipping")
			return
		}

		// Get useful tag references
		infoColumn := row.Find("td").Eq(1)
		spanTags := infoColumn.Find("span")

		// Parse circular info
		title := spanTags.First().Text()
		if title == "" {
			log.Printf("ERROR: Circular %d, has no 'title' field. Skipping\n", id)
			return
		}
		category, exist := findNodeWithContext("Categoria", spanTags.Nodes)
		if !exist {
			log.Printf("ERROR: Circular %d, has no 'category' field. Skipping\n", id)
			return
		}
		publishedDateStr, exist := findNodeWithContext("Pubblicato il", spanTags.Nodes)
		if !exist {
			log.Printf("ERROR: Circular %d, has no 'published date' field. Skipping\n", id)
			return
		}
		publishedDate, err := time.Parse("02/01/2006", publishedDateStr.Data)
		if err != nil {
			log.Printf("ERROR: Circular %d, can't parse published date. Skipping\n", id)
			return
		}
		validUntilDateStr, exist := findNodeWithContext("Valido fino", spanTags.Nodes)
		if !exist {
			log.Printf("ERROR: Circular %d, has no 'valid until' field. Skipping\n", id)
			return
		}
		validUntilDate, err := time.Parse("02/01/2006", validUntilDateStr.Data)
		if err != nil {
			log.Printf("ERROR: Circular %d, can't parse valid until date. Skipping\n", id)
			return
		}

		var attachments []attachment
		// Parse attachments, tag with class 'link-to-file' inside infoColumn
		infoColumn.Find(".link-to-file").Each(func(i int, a *goquery.Selection) {
			if idDocStr, exists := a.Attr("id_doc"); exists {
				idDoc, err := strconv.ParseUint(idDocStr, 10, 64)
				if err != nil {
					log.Printf("WARNING: can't parse circular(%d) attachment. Skipping attachment\n", id)
					return
				}
				title := a.Text()
				attachments = append(attachments, attachment{idDoc, title})
			}
		})

		// Add parsed circular to array
		circulars = append(circulars, circular{id, title, category.Data, publishedDate, validUntilDate, attachments})

		numRowResult++
	})

	return circulars, nil
}

// loadConfiguration loads db config from file
func loadConfiguration(filename string) (*dbConfig, error) {
	configFile, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	config := &dbConfig{}
	if err := json.NewDecoder(configFile).Decode(&config); err != nil {
		return nil, err
	}

	return config, nil
}

func insertCirculars(circulars []circular, numToUpdate int, connectionString string) error {
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	// Insert for each circular
	for idx, c := range circulars {
		// Updates only latest 'numToUpdate' circulars
		queryCircular := "INSERT IGNORE INTO `circolare` (id, titolo, categoria, `data`, valida_fino, aggiunta_il) VALUES (?, ?, ?, ?, ?, ?)"
		queryAttachment := "INSERT IGNORE INTO `circolare_allegato` (id_allegato, titolo, id_circolare) VALUES (?, ?, ?)"
		if idx < numToUpdate {
			queryCircular = "INSERT INTO `circolare` (id, titolo, categoria, `data`, valida_fino, aggiunta_il) VALUES (?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE titolo = VALUES(titolo), categoria = VALUES(categoria), `data` = VALUES(`data`), valida_fino = VALUES(valida_fino)"
			queryAttachment = "INSERT INTO `circolare_allegato` (id_allegato, titolo, id_circolare) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE titolo = VALUES(titolo)"
		}

		_, err = tx.Exec(
			// INSERT IGNORE would be better but circulars must not be deleted from website (not our case)
			queryCircular,
			c.Id,
			c.Title,
			c.Category,
			c.PublishedDate.Format("2006-01-02"),
			c.ValidUntilDate.Format("2006-01-02"),
			time.Now().UTC().Format(time.RFC3339))
		if err != nil {
			return err
		}

		// Insert circulars attachments
		for _, att := range c.Attachments {
			_, err = tx.Exec(
				// INSERT IGNORE would be better but circulars must not be deleted from website (not our case)
				queryAttachment,
				att.Id,
				att.Title,
				c.Id)
			if err != nil {
				return err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func deleteRemovedCirculars(circulars []circular, connectionString string) (removedCirculars, removedAttachments int, err error) {
	db, err := sql.Open("mysql", connectionString)
	if err != nil {
		return 0, 0, err
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return 0, 0, err
	}

	tx, err := db.Begin()
	if err != nil {
		return 0, 0, err
	}

	// Get parsed ids
	var parsedCircId, parsedAttachId []uint64
	for _, c := range circulars {
		parsedCircId = append(parsedCircId, c.Id)

		for _, att := range c.Attachments {
			parsedAttachId = append(parsedAttachId, att.Id)
		}
	}
	sort.Slice(parsedCircId, func(i, j int) bool { return parsedCircId[i] > parsedCircId[j] })
	sort.Slice(parsedAttachId, func(i, j int) bool { return parsedAttachId[i] > parsedAttachId[j] })

	// Get db ids
	var dbCircularsId, dbAttachmentsId []uint64
	var id uint64

	//TODO use multipleResultSets query to improve perfomance
	rowsCirculars, errC := tx.Query("SELECT id FROM circolare ORDER BY id DESC")
	if errC != nil {
		log.Fatal(errC)
	}
	defer rowsCirculars.Close()
	for rowsCirculars.Next() {
		if err := rowsCirculars.Scan(&id); err != nil {
			log.Fatal(err)
		}
		dbCircularsId = append(dbCircularsId, id)
	}

	rowsAttachments, errA := tx.Query("SELECT id_allegato id FROM circolare_allegato ORDER BY id DESC")
	if errA != nil {
		log.Fatal(errA)
	}
	defer rowsAttachments.Close()
	for rowsAttachments.Next() {
		if err := rowsAttachments.Scan(&id); err != nil {
			log.Fatal(err)
		}
		dbAttachmentsId = append(dbAttachmentsId, id)
	}

	// Search db ids that weren't parsed
	var idsCircToRemove, idsAttachToRemove []uint64
	for _, id := range dbCircularsId {
		if idx := sort.Search(len(parsedCircId), func(i int) bool { return parsedCircId[i] <= id }); parsedCircId[idx] != id {
			idsCircToRemove = append(idsCircToRemove, id)
		}
	}
	for _, id := range dbAttachmentsId {
		if idx := sort.Search(len(parsedAttachId), func(i int) bool { return parsedAttachId[i] <= id }); parsedAttachId[idx] != id {
			idsAttachToRemove = append(idsAttachToRemove, id)
		}
	}

	// Delete removed circulars
	for _, id := range idsAttachToRemove {
		tx.Exec("DELETE FROM `circolare_allegato` WHERE id_allegato = ?", id)
	}
	for _, id := range idsCircToRemove {
		tx.Exec("DELETE FROM `circolare` WHERE id = ?", id)
	}

	if err := tx.Commit(); err != nil {
		return 0, 0, err
	}

	return len(idsCircToRemove), len(idsAttachToRemove), nil
}

// Main function get the configuration from env variables and the execute the worker cycle.
// get circulars -> parse circulars -> update DB
// On a lower frequency it also remove from the DB deleted circulars
func main() {
	// Get db configs
	var connectionString string
	if envVar, exists := os.LookupEnv("CIRCULARS_DB_CONNECTION_STRING"); exists {
		connectionString = envVar
	} else {
		// Try reading form filename received as cli argument
		if argsLen := len(os.Args); argsLen < 2 {
			log.Fatal("ERROR: Missing script argument -> ./circolari <sqlcredentials-path>")
		}
		sqlConfFilename := os.Args[1]

		// Load db config
		dbConfig, err := loadConfiguration(sqlConfFilename)
		if err != nil {
			log.Fatalf("ERROR: %v", err)
		}
		connectionString = dbConfig.ConnectionString
	}

	// Get circulars siteUrl
	var siteUrl string
	if envVar, exists := os.LookupEnv("CIRCULARS_SITE_URL"); exists {
		siteUrl = envVar
	} else {
		log.Fatal("ERROR: Missing CIRCULARS_SITE_URL env variable")
	}

	// Get minutes between work cycles
	var parseTimeout time.Duration
	if envVar, exists := os.LookupEnv("CIRCULARS_CYCLE_WAIT"); exists {
		if i, err := time.ParseDuration(envVar); err == nil {
			log.Printf("INFO: duration set to %f minutes", i.Minutes())
			parseTimeout = i
		} else {
			log.Fatal("ERROR: CIRCULARS_CYCLE_WAIT isn't a parsable Duration")
		}
	} else {
		log.Fatal("ERROR: Missing CIRCULARS_CYCLE_WAIT env variable")
	}

	// First time execute without waiting
	nextTime := time.Now().UTC()
	nextCleanupTime := nextTime
	for {
		// Wait for next round
		time.Sleep(time.Until(nextTime))
		nextTime = nextTime.Truncate(time.Minute).Add(parseTimeout)

		// Get Circulars to parse
		log.Printf("INFO: getting circulars")
		circularsHtml, err := getCirculars(siteUrl)
		if err != nil {
			log.Printf("ERROR: %v", err)
			continue
		}

		// Parse circulars
		log.Printf("INFO: parsing circulars")
		circulars, err := parseCirculars(circularsHtml)
		if err != nil {
			log.Printf("ERROR: %v", err)
			continue
		}
		log.Printf("INFO: parsed %d circulars", len(circulars))

		// Updates DB
		log.Printf("INFO: updating DB")
		err = insertCirculars(circulars, 25, connectionString)
		if err != nil {
			log.Printf("ERROR: %v", err)
			continue
		}
		log.Printf("INFO: updated DB")

		// Remove deleted circulars with a lower frequency
		if nextTime.After(nextCleanupTime) {
			nextCleanupTime = nextTime.Truncate(time.Hour).Add(6 * time.Hour)

			log.Printf("INFO: removing deleted circulars")
			removedCirculars, removedAttachments, err := deleteRemovedCirculars(circulars, connectionString)
			if err != nil {
				log.Printf("ERROR: %v", err)
				continue
			}
			log.Printf("INFO: removed %d circulars and %d attachments", removedCirculars, removedAttachments)
		}

		log.Println("INFO: waiting")
	}
}
