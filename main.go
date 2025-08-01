package main

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/roylee0704/gron"
	"golang.org/x/exp/rand"
)

const (
	// UnitGbit Unit in Gigabit
	UnitGbit = 1e9
	// UnitGbyte Unit in Gigabyte
	UnitGbyte = 1e9 * 8
	// UnitMbit Unit in Megabit
	UnitMbit = 1e6
	// UnitMbyte Unit in Megabyte
	UnitMbyte = 1e6 * 8
	// UnitKbit Unit in Kilobit
	UnitKbit = 1e3
	// UnitKbyte Unit in Kilobyte
	UnitKbyte = 1e3 * 8
	// UnitBit Unit in Bit
	UnitBit = 1
	// UnitByte Unit in Byte
	UnitByte = 8

	protocol = "ssl"
	broker   = "k5aafa94.ala.asia-southeast1.emqxsl.com" // MQTT Broker address
	port     = 8883
	// topic = "t/1"
	// username = "emqx"
	// password = "******"
)

// Root represents the top-level structure of the JSON.
type Root struct {
	VnstatVersion string      `json:"vnstatversion"`
	JSONVersion   string      `json:"jsonversion"`
	Interfaces    []Interface `json:"interfaces"`
}

// Interface represents a network interface.
type Interface struct {
	Name    string     `json:"name"`
	Alias   string     `json:"alias"`
	Created DateInfo   `json:"created"`
	Updated UpdateInfo `json:"updated"`
	Traffic Traffic    `json:"traffic"`
}

// DateInfo represents the date information.
type DateInfo struct {
	Date Date `json:"date"`
}

// UpdateInfo represents the updated information.
type UpdateInfo struct {
	Date Date `json:"date"`
	Time Time `json:"time"`
}

// Date represents a date with year, month, and day.
type Date struct {
	Year  int `json:"year"`
	Month int `json:"month,omitempty"`
	Day   int `json:"day,omitempty"`
}

// Time represents time with hour and minute.
type Time struct {
	Hour   int `json:"hour"`
	Minute int `json:"minute"`
}

// Traffic represents the traffic data.
type Traffic struct {
	Total      TotalTraffic     `json:"total"`
	FiveMinute []TrafficEntry   `json:"fiveminute"`
	Hour       []TrafficEntry   `json:"hour"`
	Day        []DailyTraffic   `json:"day"`
	Month      []MonthlyTraffic `json:"month"`
	Year       []YearlyTraffic  `json:"year"`
	Top        []TopTraffic     `json:"top"`
}

// TotalTraffic represents the total traffic data.
type TotalTraffic struct {
	Rx int64 `json:"rx"`
	Tx int64 `json:"tx"`
}

// TrafficEntry represents traffic data for a specific time entry.
type TrafficEntry struct {
	ID   int   `json:"id"`
	Date Date  `json:"date"`
	Time Time  `json:"time"`
	Rx   int64 `json:"rx"`
	Tx   int64 `json:"tx"`
}

// DailyTraffic represents daily traffic data.
type DailyTraffic struct {
	ID   int   `json:"id"`
	Rx   int64 `json:"rx"`
	Tx   int64 `json:"tx"`
	Date Date  `json:"date"`
}

// MonthlyTraffic represents monthly traffic data.
type MonthlyTraffic struct {
	ID   int   `json:"id"`
	Rx   int64 `json:"rx"`
	Tx   int64 `json:"tx"`
	Date Date  `json:"date"`
}

// YearlyTraffic represents yearly traffic data.
type YearlyTraffic struct {
	ID   int   `json:"id"`
	Rx   int64 `json:"rx"`
	Tx   int64 `json:"tx"`
	Date Date  `json:"date"`
}

// TopTraffic represents the top traffic data.
type TopTraffic struct {
	ID   int   `json:"id"`
	Date Date  `json:"date"`
	Rx   int64 `json:"rx"`
	Tx   int64 `json:"tx"`
}

func createMQTTClient(username, password, caCert string) mqtt.Client {
	connectAddress := fmt.Sprintf("%s://%s:%d", protocol, broker, port)
	rand.Seed(uint64(time.Now().UnixNano()))

	clientID := fmt.Sprintf("agent-%d", rand.Int())
	fmt.Println("agent:", clientID, "conneting.")
	fmt.Println("connect address: ", connectAddress)
	opts := mqtt.NewClientOptions()
	opts.AddBroker(connectAddress)
	opts.SetUsername(username)
	opts.SetPassword(password)
	opts.SetClientID(clientID)
	opts.SetKeepAlive(time.Second * 60)
	opts.SetTLSConfig(loadTLSConfig(caCert))

	client := mqtt.NewClient(opts)
	token := client.Connect()
	if token.WaitTimeout(3*time.Second) && token.Error() != nil {
		log.Fatal(token.Error())
	}
	return client
}

func loadTLSConfig(caFile string) *tls.Config {
	// load tls config
	var tlsConfig tls.Config
	tlsConfig.InsecureSkipVerify = false
	if caFile != "" {
		certpool := x509.NewCertPool()
		ca, err := ioutil.ReadFile(caFile)
		if err != nil {
			log.Fatal(err.Error())
		}
		certpool.AppendCertsFromPEM(ca)
		tlsConfig.RootCAs = certpool
	}
	return &tlsConfig
}

func publish(client mqtt.Client, topic, payload string) {
	qos := 0
	// payload := fmt.Sprintf("message: %d!", msgCount)
	if token := client.Publish(topic, byte(qos), false, payload); token.Wait() && token.Error() != nil {
		fmt.Printf("error: %s\n", token.Error())
		fmt.Printf("publish failed, topic: %s, payload: %s\n", topic, payload)
	} else {
		fmt.Printf("publish success, topic: %s, payload: %s\n", topic, payload)
	}
}

func main() {

	vnstatPath := os.Getenv("VNSTAT_PATH")
	if vnstatPath == "" {
		fmt.Println("VNSTAT_PATH environment required.")
		os.Exit(1)
	}
	mqttTopic := os.Getenv("MQTT_TOPIC")
	if mqttTopic == "" {
		fmt.Println("MQTT_TOPIC environment required.")
		os.Exit(1)
	}
	mqttUser := os.Getenv("MQTT_USER")
	if mqttUser == "" {
		fmt.Println("MQTT_USER environment required.")
		os.Exit(1)
	}
	mqttPass := os.Getenv("MQTT_PASSWORD")
	if mqttPass == "" {
		fmt.Println("MQTT_PASSWORD environment required.")
		os.Exit(1)
	}
	mqttSSLPath := os.Getenv("MQTT_SSL_PATH")
	if mqttSSLPath == "" {
		fmt.Println("MQTT_SSL_PATH environment required.")
		os.Exit(1)
	}
	vnstatJSONMode := os.Getenv("VNSTAT_JSON_MODE")
	if vnstatJSONMode == "" {
		vnstatJSONMode = "m"
	}
	vnstatInterface := os.Getenv("VNSTAT_INTERFACE")
	if vnstatInterface == "" {
		fmt.Println("VNSTAT_INTERFACE environment required.")
		os.Exit(2)
	}

	client := createMQTTClient(mqttUser, mqttPass, mqttSSLPath)

	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool, 1)

	go func() {

		sig := <-sigs
		client.Disconnect(250)
		fmt.Println()
		fmt.Println(sig)
		done <- true
	}()

	halfMinute := gron.Every(30 * time.Second)
	cron := gron.New()
	// cron.AddFunc(midnight, func() {
	// 	log.Println("Refresing materialized view ppmb.v_ppmb_ref_filter")
	// 	_, err := Dbc.Con.Exec("refresh materialized view ppmb.v_ppmb_ref_filter")
	// 	if err != nil {
	// 		log.Printf("err refresh materialized view: %+v\n", err)
	// 	}
	// })
	cron.AddFunc(halfMinute, func() {
		var root Root
		bjsonStat, err := exec.Command(vnstatPath, "-i", vnstatInterface, "--json", vnstatJSONMode, "1").Output()
		if err != nil {
			fmt.Println("Error invoking vnstat:", err)
			return
		}

		json.Unmarshal(bjsonStat, &root)
		total := float64(root.Interfaces[0].Traffic.Month[0].Rx) + float64(root.Interfaces[0].Traffic.Month[0].Tx)
		out := total / float64(UnitGbyte)
		publish(client, mqttTopic, fmt.Sprintf("%18f", out))
		fmt.Printf("Parsed struct: %18f\n", out)
	})
	cron.Start()

	<-done
	fmt.Println("exiting")
}
