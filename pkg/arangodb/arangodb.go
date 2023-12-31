package arangodb

import (
	"context"
	"encoding/json"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/jalapeno/topology/pkg/dbclient"
	notifier "github.com/jalapeno/topology/pkg/kafkanotifier"
	"github.com/sbezverk/gobmp/pkg/bmp"
	"github.com/sbezverk/gobmp/pkg/message"
	"github.com/sbezverk/gobmp/pkg/tools"
)

type arangoDB struct {
	dbclient.DB
	*ArangoConn
	stop            chan struct{}
	unicastprefixV4 driver.Collection
	unicastprefixV6 driver.Collection
	inetprefixV4    driver.Collection
	inetprefixV6    driver.Collection
}

// NewDBSrvClient returns an instance of a DB server client process
func NewDBSrvClient(arangoSrv, user, pass, dbname, unicastprefixV4, unicastprefixV6,
	inetprefixV4 string, inetprefixV6 string) (dbclient.Srv, error) {
	if err := tools.URLAddrValidation(arangoSrv); err != nil {
		return nil, err
	}
	arangoConn, err := NewArango(ArangoConfig{
		URL:      arangoSrv,
		User:     user,
		Password: pass,
		Database: dbname,
	})
	if err != nil {
		return nil, err
	}
	arango := &arangoDB{
		stop: make(chan struct{}),
	}
	arango.DB = arango
	arango.ArangoConn = arangoConn

	// Check if vertex collection exists, if not fail as Jalapeno topology is not running
	arango.unicastprefixV4, err = arango.db.Collection(context.TODO(), unicastprefixV4)
	if err != nil {
		return nil, err
	}
	// Check if vertex collection exists, if not fail as Jalapeno ipv4_topology is not running
	arango.unicastprefixV6, err = arango.db.Collection(context.TODO(), unicastprefixV6)
	if err != nil {
		return nil, err
	}
	// check for inet4 collection
	found, err := arango.db.CollectionExists(context.TODO(), inetprefixV4)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), inetprefixV4)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}
	// check for inet6 collection
	found, err = arango.db.CollectionExists(context.TODO(), inetprefixV6)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), inetprefixV6)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}
	// create unicast prefix V4 edge collection
	var inetV4_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.inetprefixV4, err = arango.db.CreateCollection(context.TODO(), "inet_prefix_v4", inetV4_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.inetprefixV4, err = arango.db.Collection(context.TODO(), inetprefixV4)
	if err != nil {
		return nil, err
	}
	// create unicast prefix V6 edge collection
	var inetV6_options = &driver.CreateCollectionOptions{ /* ... */ }
	arango.inetprefixV6, err = arango.db.CreateCollection(context.TODO(), "inet_prefix_v6", inetV6_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.inetprefixV6, err = arango.db.Collection(context.TODO(), inetprefixV6)
	if err != nil {
		return nil, err
	}
	return arango, nil
}

func (a *arangoDB) Start() error {
	if err := a.loadCollection(); err != nil {
		return err
	}
	glog.Infof("Connected to arango database, starting monitor")
	go a.monitor()

	return nil
}

func (a *arangoDB) Stop() error {
	close(a.stop)

	return nil
}

func (a *arangoDB) GetInterface() dbclient.DB {
	return a.DB
}

func (a *arangoDB) GetArangoDBInterface() *ArangoConn {
	return a.ArangoConn
}

func (a *arangoDB) StoreMessage(msgType dbclient.CollectionType, msg []byte) error {
	event := &notifier.EventMessage{}
	if err := json.Unmarshal(msg, event); err != nil {
		return err
	}
	event.TopicType = msgType
	switch msgType {
	case bmp.UnicastPrefixV4Msg:
		return a.unicastV4Handler(event)
	case bmp.UnicastPrefixV6Msg:
		return a.unicastV6Handler(event)
	}
	return nil
}

func (a *arangoDB) monitor() {
	for {
		select {
		case <-a.stop:
			return
		}
	}
}

func (a *arangoDB) loadCollection() error {
	ctx := context.TODO()
	v4_query := "for l in unicast_prefix_v4 filter l.prefix_len < 26 " +
		"filter l.remote_asn != l.origin_as filter l.base_attrs.local_pref == null return l"
	cursor, err := a.db.Query(ctx, v4_query, nil)
	glog.Infof("execute v4 query %+v", v4_query)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.UnicastPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		if err := a.processInet4(ctx, meta.Key, meta.ID.String(), p); err != nil {
			glog.Errorf("Failed to process prefix %s with error: %+v", p.ID, err)
			continue
		}
	}
	v6_query := "for l in unicast_prefix_v6 filter l.prefix_len < 80 " +
		"filter l.remote_asn != l.origin_as filter l.base_attrs.local_pref == null return l"
	cursor, err = a.db.Query(ctx, v6_query, nil)
	glog.Infof("execute v6 query %+v", v6_query)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.UnicastPrefix
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		if err := a.processInet6(ctx, meta.Key, meta.ID.String(), p); err != nil {
			glog.Errorf("Failed to process prefix %s with error: %+v", p.ID, err)
			continue
		}
	}
	return nil
}
