package arangodb

import (
	"context"
	"strconv"

	driver "github.com/arangodb/go-driver"
	"github.com/golang/glog"
	"github.com/sbezverk/gobmp/pkg/message"
)

// func (a *arangoDB) loadInet4(ctx context.Context, key string, e *message.UnicastPrefix) error {
// 	glog.Infof("processing prefix as %+v", e.Prefix+"_"+strconv.Itoa(int(e.PrefixLen)))

// 	ne := inetPrefix{
// 		Key:       e.Prefix + "_" + strconv.Itoa(int(e.PrefixLen)),
// 		Prefix:    e.Prefix,
// 		PrefixLen: e.PrefixLen,
// 		OriginAS:  e.OriginAS,
// 	}

// 	if _, err := a.inetprefixV4.CreateDocument(ctx, ne); err != nil {
// 		glog.Infof("adding inet4 prefix: %+v ", ne)
// 		if !driver.IsConflict(err) {
// 			return err
// 		}
// 		// The document already exists, updating it with the latest info
// 		if _, err := a.inetprefixV4.UpdateDocument(ctx, ne.Key, e); err != nil {
// 			return err
// 		}
// 		return nil
// 	}

// 	if err := a.processInet4(ctx, ne.Key, e); err != nil {
// 		glog.Errorf("Failed to process inet4 prefix %s with error: %+v", ne.Key, err)
// 	}
// 	return nil
// }

func (a *arangoDB) processInet4(ctx context.Context, key string, e *message.UnicastPrefix) error {
	inet4_query := "for l in unicast_prefix_v4 " +
		"filter l.prefix_len < 25 filter l.remote_asn != l.origin_as filter l.base_attrs.local_pref == null "
	inet4_query += "return { prefix: l.prefix, prefix_len: l.prefix_len, origin_as: l.origin_as } "
	//inet4_query += "return l "
	cursor, err := a.db.Query(ctx, inet4_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.UnicastPrefix
		e, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		glog.Infof("processing prefix %+v as %+v", p.Key, p.Prefix+"_"+strconv.Itoa(int(p.PrefixLen)))

		ne := inetPrefix{
			Key:       p.Prefix + "_" + strconv.Itoa(int(p.PrefixLen)),
			Prefix:    p.Prefix,
			PrefixLen: p.PrefixLen,
			OriginAS:  p.OriginAS,
		}
		if _, err := a.inetprefixV4.CreateDocument(ctx, ne); err != nil {
			glog.Infof("adding inet4 prefix: %+v via %+v ", ne, a.inetprefixV4)
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.inetprefixV4.UpdateDocument(ctx, ne.Key, e); err != nil {
				return err
			}
			return nil
		}

		if err := a.processInet4(ctx, ne.Key, &p); err != nil {
			glog.Errorf("Failed to process inet4 prefix %s with error: %+v", ne.Key, err)
		}
	}

	// 	query := "for l in  " + a.unicastprefixV4.Name() +
	// 		" filter l.prefix_len < 26 "
	// 	query += " return l"
	// 	ncursor, err := a.db.Query(ctx, key, nil)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	defer ncursor.Close()
	// 	var sn inetPrefix
	// 	ns, err := ncursor.ReadDocument(ctx, &sn)
	// 	if err != nil {
	// 		if !driver.IsNoMoreDocuments(err) {
	// 			return err
	// 		}
	// 	}

	// 	ne := inetPrefix{
	// 		Key:       e.Prefix + "_" + strconv.Itoa(int(e.PrefixLen)),
	// 		Prefix:    e.Prefix,
	// 		PrefixLen: e.PrefixLen,

	// 		OriginAS: e.OriginAS,
	// 	}

	// 	if _, err := a.inetprefixV4.CreateDocument(ctx, &ne); err != nil {
	// 		glog.Infof("adding inet4 prefix: %s ", ns.Key)
	// 		if !driver.IsConflict(err) {
	// 			return err
	// 		}
	// 		// The document already exists, updating it with the latest info
	// 		if _, err := a.inetprefixV4.UpdateDocument(ctx, ne.Key, e); err != nil {
	// 			return err
	// 		}
	// 		return nil
	// 	}

	// 	if err := a.processInet4(ctx, ne.Key, e); err != nil {
	// 		glog.Errorf("Failed to process ls_node_extended %s with error: %+v", ne.Key, err)
	// 	}
	return nil
}

func (a *arangoDB) processInet4b(ctx context.Context, key, id string, e message.UnicastPrefix) error {
	inet4_query := "for l in unicast_prefix_v4 " +
		"filter l.prefix_len < 25 filter l.remote_asn != l.origin_as filter l.base_attrs.local_pref == null "
	//inet4_query += "return { prefix: l.prefix, prefix_len: l.prefix_len, origin_as: l.origin_as } "
	inet4_query += "return l "
	pcursor, err := a.db.Query(ctx, inet4_query, nil)
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
		glog.Infof("prefix: %+v + as prefix %v  ", ln.Key, ln.Prefix)

		ne := inetPrefix{
			Key:       e.Prefix + "_" + strconv.Itoa(int(e.PrefixLen)),
			Prefix:    e.Prefix,
			PrefixLen: e.PrefixLen,
			OriginAS:  e.OriginAS,
		}
		if _, err := a.inetprefixV4.CreateDocument(ctx, &ne); err != nil {
			glog.Infof("adding inet4 prefix: %s ", nl.Key)
			if !driver.IsConflict(err) {
				return err
			}
		}
		if _, err := a.inetprefixV4.UpdateDocument(ctx, nl.Key, &ne); err != nil {
			if !driver.IsConflict(err) {
				return err
			}
		}
	}
	return nil
}

// process removes records from the inet4 collection
func (a *arangoDB) processPrefixV4Removal(ctx context.Context, key string) error {
	query := "FOR d IN " + a.inetprefixV4.Name() +
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

func (a *arangoDB) processInet6(ctx context.Context, key string, e *message.UnicastPrefix) error {
	inet6_query := "for l in unicast_prefix_v6 " +
		"filter l.prefix_len < 80 filter l.remote_asn != l.origin_as filter l.base_attrs.local_pref == null "
	inet6_query += "return l "
	cursor, err := a.db.Query(ctx, inet6_query, nil)
	if err != nil {
		return err
	}
	defer cursor.Close()
	for {
		var p message.UnicastPrefix
		e, err := cursor.ReadDocument(ctx, &p)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return err
		}
		glog.Infof("processing prefix as %+v", p.Prefix+"_"+strconv.Itoa(int(p.PrefixLen)))

		ne := inetPrefix{
			Key:       p.Prefix + "_" + strconv.Itoa(int(p.PrefixLen)),
			Prefix:    p.Prefix,
			PrefixLen: p.PrefixLen,
			OriginAS:  p.OriginAS,
		}
		if _, err := a.inetprefixV6.CreateDocument(ctx, ne); err != nil {
			glog.Infof("adding inet6 prefix: %+v ", ne)
			if !driver.IsConflict(err) {
				return err
			}
			// The document already exists, updating it with the latest info
			if _, err := a.inetprefixV6.UpdateDocument(ctx, ne.Key, e); err != nil {
				return err
			}
			return nil
		}

		if err := a.processInet6(ctx, ne.Key, &p); err != nil {
			glog.Errorf("Failed to process inet6 prefix %s with error: %+v", ne.Key, err)
		}
	}
	return nil
}

// process removes records from the inet4 collection
func (a *arangoDB) processPrefixV6Removal(ctx context.Context, key string) error {
	query := "FOR d IN " + a.inetprefixV6.Name() +
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
