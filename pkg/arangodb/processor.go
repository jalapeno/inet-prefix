package arangodb

import (
	"context"
	"strconv"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

func (a *arangoDB) processInet4(ctx context.Context, key, id string, e message.UnicastPrefix) error {
	query := "for l in unicast_prefix_v4 filter l.prefix_len < 26 " +
		"filter l.remote_asn != l.origin_as filter l.base_attrs.local_pref == null return l"
	pcursor, err := a.db.Query(ctx, query, nil)
	glog.Infof("processor query: %+v", query)
	if err != nil {
		return err
	}
	defer pcursor.Close()
	for {
		var ln inetPrefix
		nl, err := pcursor.ReadDocument(ctx, &ln)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		glog.Infof("document %+v, prefix %v", e.Key, e.Prefix)

		obj := inetPrefix{
			Key:       e.Prefix + "_" + strconv.Itoa(int(e.PrefixLen)),
			Prefix:    e.Prefix,
			PrefixLen: e.PrefixLen,
			OriginAS:  e.OriginAS,
		}
		if _, err := a.inetprefixV4.CreateDocument(ctx, &obj); err != nil {
			glog.Infof("adding prefix: %+v", e.Prefix+"_"+strconv.Itoa(int(e.PrefixLen)))
			if !driver.IsConflict(err) {
				return err
			}
		}
		// The document already exists, updating it with the latest info
		if _, err := a.inetprefixV4.UpdateDocument(ctx, nl.Key, &obj); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
		}
	}
	return nil
}

// process Removal removes records from the inetprefixV4 collection
func (a *arangoDB) processInet4Removal(ctx context.Context, key string) error {
	query := "for d in " + a.inetprefixV4.Name() +
		" filter d._key == " + "\"" + key + "\""
	query += " return d"
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()

	for {
		var nm inetPrefix
		m, err := ncursor.ReadDocument(ctx, &nm)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		if _, err := a.inetprefixV4.RemoveDocument(ctx, m.ID.Key()); err != nil {
			if !driver.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}

func (a *arangoDB) processInet6(ctx context.Context, key, id string, e message.UnicastPrefix) error {
	query := "for l in unicast_prefix_v6 filter l.prefix_len < 80 " +
		"filter l.remote_asn != l.origin_as filter l.base_attrs.local_pref == null return l"
	pcursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer pcursor.Close()
	for {
		var ln inetPrefix
		nl, err := pcursor.ReadDocument(ctx, &ln)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		//glog.Infof("document %+v, prefix %v", e.Key, e.Prefix)

		obj := inetPrefix{
			Key:       e.Prefix + "_" + strconv.Itoa(int(e.PrefixLen)),
			Prefix:    e.Prefix,
			PrefixLen: e.PrefixLen,
			OriginAS:  e.OriginAS,
		}
		if _, err := a.inetprefixV6.CreateDocument(ctx, &obj); err != nil {
			//glog.Infof("adding prefix: %+v", e.Prefix+"_"+strconv.Itoa(int(e.PrefixLen)))
			if !driver.IsConflict(err) {
				return err
			}
		}
		// The document already exists, updating it with the latest info
		if _, err := a.inetprefixV6.UpdateDocument(ctx, nl.Key, &obj); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
		}
	}
	return nil
}

// process Removal removes records from the inetprefixV4 collection
func (a *arangoDB) processInet6Removal(ctx context.Context, key string) error {
	query := "for d in " + a.inetprefixV4.Name() +
		" filter d._key == " + "\"" + key + "\""
	query += " return d"
	ncursor, err := a.db.Query(ctx, query, nil)
	if err != nil {
		return err
	}
	defer ncursor.Close()

	for {
		var nm inetPrefix
		m, err := ncursor.ReadDocument(ctx, &nm)
		if err != nil {
			if !driver.IsNoMoreDocuments(err) {
				return err
			}
			break
		}
		if _, err := a.inetprefixV6.RemoveDocument(ctx, m.ID.Key()); err != nil {
			if !driver.IsNotFound(err) {
				return err
			}
		}
	}
	return nil
}
