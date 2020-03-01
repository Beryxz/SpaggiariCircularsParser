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
	"strconv"
	"strings"
	"time"
)

type moreCircolariMsg struct {
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
	Id uint64
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

// getCircolari returns all the circulars from the "segreteria digitale of itismeucci" as parsable html
func getCirculars() (*strings.Reader, error) {
	siteUrl := "https://web.spaggiari.eu/sdg/app/default/comunicati.php?sede_codice=XXXX0000"
	client := &http.Client{}
	count := 0
	circolariHtml := ""

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

		var m moreCircolariMsg
		if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
			return nil, errors.New("can't parse response body")
		}
		resp.Body.Close()

		circolariHtml += m.Htm
		if m.Cnt <= 0 {
			break
		}
		count += 100
	}

	return strings.NewReader("<html><body><table>" + circolariHtml + "</table></body></html>"), nil
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

func parseCirculars(circularsHtml *strings.Reader) (circulars []circular, canceled []circular, err error) {
	numRowResult := 0

	// Load the HTML doc
	doc, err := goquery.NewDocumentFromReader(circularsHtml)
	if err != nil {
		return nil, nil, err
	}

	// Parse single circular
	doc.Find("tr.row-result").Each(func(i int, row *goquery.Selection) {
		// Parse circular ID
		var id uint64
		idStr, exist := row.Find(".download-file").Attr("id_doc")
		noId := !exist
		if !noId {
			id, err = strconv.ParseUint(idStr, 10, 64)
			if err != nil {
				log.Println("ERROR: can't parse id to int. Skipping")
				return
			}
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

		if !noId {
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
		} else {
			canceled = append(canceled, circular{0, title, category.Data, publishedDate, validUntilDate, nil})
		}
		numRowResult++
	})

	return circulars, canceled,nil
}

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

func insertCirculars(circulars []circular, canceled []circular, connectionString string) error {
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
	for _, c := range circulars {
		_, err = tx.Exec(
			"INSERT IGNORE INTO `circolare` (id, titolo, categoria, `data`, valida_fino, aggiunta_il) VALUES (?, ?, ?, ?, ?, ?)",
			c.Id,
			c.Title,
			c.Category,
			c.PublishedDate.Format("2006-01-02"),
			c.ValidUntilDate.Format("2006-01-02"),
			time.Now().Format(time.RFC3339))
		if err != nil {
			return err
		}

		// Insert circulars attachments
		for _, att := range c.Attachments {
			_, err = tx.Exec(
				"INSERT IGNORE INTO `circolare_allegato` (id_allegato, titolo, id_circolare) VALUES (?, ?, ?)",
				att.Id,
				att.Title,
				c.Id)
			if err != nil {
				return err
			}
		}
	}

	// Try to remove circulars that are now canceled
	for _, c := range canceled {
		_, err = tx.Exec(
			"DELETE FROM `circolare` WHERE titolo = ? AND categoria = ? AND `data` = ? AND valida_fino = ?,",
			c.Title,
			c.Category,
			c.PublishedDate.Format("2006-01-02"),
			c.ValidUntilDate.Format("2006-01-02"))
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func main() {
	var connectionString string
	if envVar, exists := os.LookupEnv("DB_CONNECTION_STRING"); exists {
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

	// First time execute without waiting
	nextTime := time.Now()
	// Execute forever in a loop
	for {
		// Wait for next round
		time.Sleep(time.Until(nextTime))
		nextTime = time.Now().Truncate(time.Minute).Add(5 * time.Minute)

		// Get Circulars to parse
		log.Printf("INFO: getting circulars")
		circularsHtml, err := getCirculars()
		if err != nil {
			log.Printf("ERROR: %v", err)
			continue
		}

		// Parse circulars
		log.Printf("INFO: parsing circulars")
		circulars, canceled, err := parseCirculars(circularsHtml)
		if err != nil {
			log.Printf("ERROR: %v", err)
			continue
		}
		log.Printf("INFO: parsed %d circulars\n", len(circulars) + len(canceled))

		// Updates DB
		log.Printf("INFO: updating DB")
		err = insertCirculars(circulars, canceled, connectionString)
		if err != nil {
			log.Printf("ERROR: %v", err)
			continue
		}
		log.Println("INFO: updated DB")

		log.Println("INFO: waiting")
	}
}
