package main

import (
	"bytes"
	"encoding/csv"
	"log"
	"net/http"
	"os"
	"strings"
)

func init() {
	log.SetFlags(0)
}

func main() {

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	// http handler
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		HttpHandler(w, r)
	})

	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// HttpHandler Handles the HTTP call
func HttpHandler(w http.ResponseWriter, r *http.Request) {
	bucketname := os.Getenv("bucketname")

	log.Printf("Recieved event: %s", r.Header.Get("Ce-Methodname"))

	if string(r.Header.Get("Ce-Methodname")) == "storage.objects.create" {
		// obj looks like this; storage.googleapis.com/projects/_/buckets/webhook-looker-6814/objects/us_new_users.csv
		obj := string(r.Header.Get("Ce-Subject"))
		obj = strings.ReplaceAll(obj, "storage.googleapis.com/projects/_/buckets/", "")
		obj = strings.ReplaceAll(obj, bucketname, "")
		obj = strings.ReplaceAll(obj, "/objects/", "")

		log.Printf("Fetching object: %s\n", obj)
		f, err := gcsDownloadFile(bucketname, obj)
		if err != nil {
			log.Printf("Could not retrieve the object %s, error: %v", obj, err)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		// read csv values using csv.Reader
		r := bytes.NewReader(f)
		csvReader := csv.NewReader(r)
		data, err := csvReader.ReadAll()
		if err != nil {
			log.Printf("Cloud not parse the CSV object : %v", err)
			w.WriteHeader(http.StatusInternalServerError)
		}

		err = UpdateSheets(obj, data)
		if err != nil {
			log.Printf("Cloud not update the spreadsheet : %v", err)
			w.WriteHeader(http.StatusInternalServerError)

		}

		

	} else {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}

	w.WriteHeader(http.StatusOK)
}
