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
	stop                chan struct{}
	peer                driver.Collection
	peerEdgeV4          driver.Collection
	peerEdgeV6          driver.Collection
	unicastprefixV4     driver.Collection
	unicastprefixV6     driver.Collection
	unicastprefixEdgeV4 driver.Collection
	unicastprefixEdgeV6 driver.Collection
}

// NewDBSrvClient returns an instance of a DB server client process
func NewDBSrvClient(arangoSrv, user, pass, dbname, peer, unicastprefixV4, unicastprefixV6,
	peerEdgeV4 string, peerEdgeV6 string, unicastprefixEdgeV4 string, unicastprefixEdgeV6 string) (dbclient.Srv, error) {
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
	arango.peer, err = arango.db.Collection(context.TODO(), peer)
	if err != nil {
		return nil, err
	}
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

	// check for peer edge collection
	found, err := arango.db.CollectionExists(context.TODO(), peerEdgeV4)
	if err != nil {
		return nil, err
	}
	if found {
		c, err := arango.db.Collection(context.TODO(), peerEdgeV4)
		if err != nil {
			return nil, err
		}
		if err := c.Remove(context.TODO()); err != nil {
			return nil, err
		}
	}

	// create peer edge v4 collection
	var peerEdgeV4_options = &driver.CreateCollectionOptions{ /* ... */ }
	glog.Infof("peer edge v4 not found, creating")
	arango.peerEdgeV4, err = arango.db.CreateCollection(context.TODO(), "peer_edge_v4", peerEdgeV4_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.peerEdgeV4, err = arango.db.Collection(context.TODO(), peerEdgeV4)
	if err != nil {
		return nil, err
	}

	// create peer edge v6 collection
	var peerEdgeV6_options = &driver.CreateCollectionOptions{ /* ... */ }
	glog.Infof("peer edge v6 not found, creating")
	arango.peerEdgeV4, err = arango.db.CreateCollection(context.TODO(), "peer_edge_v6", peerEdgeV6_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.peerEdgeV6, err = arango.db.Collection(context.TODO(), peerEdgeV6)
	if err != nil {
		return nil, err
	}

	// create unicast prefix V4 edge collection
	var unicastprefixEdgeV4_options = &driver.CreateCollectionOptions{ /* ... */ }
	glog.Infof("unicast prefix v4 edge not found, creating")
	arango.unicastprefixEdgeV4, err = arango.db.CreateCollection(context.TODO(), "unicast_prefix_edge_v4", unicastprefixEdgeV4_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.unicastprefixEdgeV4, err = arango.db.Collection(context.TODO(), unicastprefixEdgeV4)
	if err != nil {
		return nil, err
	}

	// create unicast prefix V6 edge collection
	var unicastprefixEdgeV6_options = &driver.CreateCollectionOptions{ /* ... */ }
	glog.Infof("unicast prefix v6 edge not found, creating")
	arango.unicastprefixEdgeV6, err = arango.db.CreateCollection(context.TODO(), "unicast_prefix_edge_v6", unicastprefixEdgeV6_options)
	if err != nil {
		return nil, err
	}
	// check if collection exists, if not fail as processor has failed to create collection
	arango.unicastprefixEdgeV6, err = arango.db.Collection(context.TODO(), unicastprefixEdgeV6)
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
	case bmp.PeerStateChangeMsg:
		return a.peerHandler(event)
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
			// TODO Add clean up of connection with Arango DB
			return
		}
	}
}

func (a *arangoDB) loadCollection() error {
	ctx := context.TODO()
	peer_query := "for l in " + a.peer.Name() + " filter l.local_ip !like " + "\"%:%\"" + " return l "
	cursor, err := a.db.Query(ctx, peer_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.PeerStateChange
		//var p LSPfx
		meta, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		if err := a.processPeer(ctx, meta.Key, meta.ID.String(), p); err != nil {
			glog.Errorf("Failed to process ls_prefix_sid %s with error: %+v", p.ID, err)
		}
	}

	return nil
}
