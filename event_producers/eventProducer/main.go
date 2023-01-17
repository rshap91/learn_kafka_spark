package main

//go:generate msgp
import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/linkedin/goavro"
)

// BASE
type EventType int64

const (
	AdView    EventType = 0
	AdClick             = 1
	ItemView            = 2
	ItemSave            = 3
	ItemOrder           = 4
)

const NUMUSERS int = 1
const NUMITEMS int = 10000
const NUMADSPERITEM int = 5

const (
	SEESADPROB         float32 = 0.5
	ADCLICKPROB                = 0.1
	ITEMSAVEPROB               = 0.2
	ITEMORDERPROB              = 0.05
	SAVEDITEMORDERPROB         = 0.15
)

const (
	USERMAXWAITTIME        int = 5  // time between events within a flow
	USRMAXTIMEBETWEENFLOWS int = 30 // time between starting a new flow
)

const AVROSCHEMA string = `{
    "namespace": "user_events.serialization.avro",
    "name": "Event",
    "type": "record",
    "fields": [
        {"name": "EventId", "type": "string"},
        {"name": "UserId", "type":  "int"},
        {"name": "AdId", "type":    ["null", "int"],
		 "default": null},
        {"name": "ItemId", "type":  "int"},
        {"name": "Ts", "type":      "long", 
		 "logicalType": 	"timestamp-micros"},
        {"name": "Kind", "type":    "int"}
    ]
}`

var EVENTTOPIC string = "userEvents"

type Event struct {
	EventId string
	UserId  int
	AdId    *int
	ItemId  int
	Ts      int64
	Kind    EventType
}

// Avro has specific rules for json of Union Types (eg AdId where value can be null)
type avroCompatibleEvent struct {
	EventId string
	UserId  int
	AdId    map[string]*int
	ItemId  int
	Ts      int64
	Kind    EventType
}

func generateItemId() int {
	return rand.Intn(NUMITEMS)
}

func generateAdId(ItemId int) *int {
	adId := ItemId*NUMADSPERITEM + rand.Intn(NUMADSPERITEM)
	return &adId
}

type Ad struct {
	AdId   int
	ItemId int
}

type Item struct {
	ItemId      int
	Category    int
	SubCategory int
	Price       float64
}

type User struct {
	UserId           int
	SavedItems       map[int]struct{}
	IsBot            bool
	eventCounter     int
	timeBetweenFLows int
	kafkaProducer    *kafka.Producer
}

func SerializeEvent(evt Event) []byte {

	codec, err := goavro.NewCodec(AVROSCHEMA)
	if err != nil {
		fmt.Println(err)
	}

	// handle json requirements for avro json
	var avroAdId map[string]*int
	if evt.AdId != nil {
		avroAdId = map[string]*int{"int": evt.AdId}
	}
	avroEvt := avroCompatibleEvent{
		evt.EventId,
		evt.UserId,
		avroAdId,
		evt.ItemId,
		evt.Ts,
		evt.Kind,
	}

	// event to json
	jbytes, err := json.Marshal(&avroEvt)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("JSON", string(jbytes))

	// json to native
	native, _, err := codec.NativeFromTextual(jbytes)
	if err != nil {
		fmt.Println(err)
	}

	// native to avro binary
	binary, err := codec.BinaryFromNative(nil, native)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("%T: %v\n", binary, binary)

	return binary
}

func (usr *User) postEvents(evts *[]Event) {
	fmt.Println("Posting Events For User ", usr.UserId)
	for _, evt := range *evts {
		fmt.Printf("EventId: %s, UserId: %d, ItemId: %d, Kind: %d\n", evt.EventId, evt.UserId, evt.ItemId, evt.Kind)
		data := SerializeEvent(evt)
		msg := kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &EVENTTOPIC, Partition: kafka.PartitionAny},
			Value:          data,
		}
		fmt.Println(msg)
		// This is async
		// Can handle results by passing a channel as second arg to receive results or by using usr.kafkaProducer.Events() channel
		usr.kafkaProducer.Produce(&msg, nil)
	}

}

func (usr *User) GenerateEvent(adId *int, itemId int, kind EventType) Event {
	usr.eventCounter++
	return Event{
		fmt.Sprint(usr.UserId) + "~" + fmt.Sprint(usr.eventCounter),
		usr.UserId,
		adId,
		itemId,
		time.Now().UnixMicro(),
		kind,
	}
}

func (usr *User) ProduceEventFlow() {
	var events []Event
	defer usr.postEvents(&events)

	itemId := generateItemId()
	seesAd := rand.Float32() < SEESADPROB
	var adId *int = nil
	if seesAd {
		adId = generateAdId(itemId)
		adViewEvt := usr.GenerateEvent(adId, itemId, AdView)
		events = append(events, adViewEvt)

		if !usr.IsBot {
			time.Sleep(time.Duration(rand.Intn(USERMAXWAITTIME)+1) * time.Second)
		}

		clicksAd := rand.Float32() < ADCLICKPROB
		if clicksAd {
			adClickEvt := usr.GenerateEvent(adId, itemId, AdClick)
			events = append(events, adClickEvt)
		} else {
			return
		}
	}

	viewItemEvt := usr.GenerateEvent(adId, itemId, ItemView)
	events = append(events, viewItemEvt)
	if !usr.IsBot {
		time.Sleep(time.Duration(rand.Intn(USERMAXWAITTIME)+1) * time.Second)
	}
	_, itemAlreadySaved := usr.SavedItems[itemId]
	savesItem := rand.Float32() < ITEMSAVEPROB

	if savesItem && !itemAlreadySaved {
		saveItemEvt := usr.GenerateEvent(adId, itemId, ItemSave)
		events = append(events, saveItemEvt)
		usr.SavedItems[itemId] = struct{}{}
		return
	}

	var ordersItem bool
	if itemAlreadySaved {
		ordersItem = rand.Float32() < SAVEDITEMORDERPROB
	} else {
		ordersItem = rand.Float32() < ITEMORDERPROB
	}

	if ordersItem {
		orderItemEvt := usr.GenerateEvent(adId, itemId, ItemOrder)
		events = append(events, orderItemEvt)
	}

	return

}

func (usr *User) Run() {
	defer usr.kafkaProducer.Close()
	defer usr.kafkaProducer.Flush(1500)
	fmt.Println("User ", usr.UserId, " Running")
	for {
		usr.ProduceEventFlow()
		time.Sleep(time.Duration(rand.Intn(usr.timeBetweenFLows)) * time.Second)
	}

}

func main() {
	for i := 1; i < NUMUSERS+1; i++ {
		p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9093"})
		if err != nil {
			panic(err)
		}
		// var p kafka.Producer
		usr := User{
			UserId:           i,
			SavedItems:       make(map[int]struct{}),
			IsBot:            false,
			eventCounter:     1,
			timeBetweenFLows: rand.Intn(USRMAXTIMEBETWEENFLOWS) + 1,
			kafkaProducer:    p,
		}
		go usr.Run()
	}

	var stop string
	fmt.Scanln(&stop)

}
