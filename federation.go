package rabbithole

import (
	"encoding/json"
	"fmt"
	"net/http"

	"bitbucket.org/unchain/unchain-ares-old/logger"
	"github.com/pkg/errors"
)

type FederationSetDefinition struct {
	Upstream string `json:"upstream"`
}

// Federation definition: additional arguments
// added to the entities (queues, exchanges or both)
// that match a policy.
type FederationDefinition struct {
	Uri            string `json:"uri"`
	Expires        int    `json:"expires"`
	MessageTTL     int32  `json:"message-ttl"`
	MaxHops        int    `json:"max-hops"`
	PrefetchCount  int    `json:"prefetch-count"`
	ReconnectDelay int    `json:"reconnect-delay"`
	AckMode        string `json:"ack-mode,omitempty"`
	TrustUserId    bool   `json:"trust-user-id"`
	Exchange       string `json:"exchange"`
	Queue          string `json:"queue"`
}

// Represents a configured Federation upstream.
type FederationUpstream struct {
	Definition FederationDefinition `json:"value"`
}

type FederationUpstreamSet struct {
	Definition []FederationSetDefinition `json:"value"`
}

//
// PUT /api/parameters/federation-upstream/{vhost}/{upstream}
//

// Updates a federation upstream
func (c *Client) PutFederationUpstream(vhost string, upstreamName string, fDef FederationDefinition) (res *http.Response, err error) {
	fedUp := FederationUpstream{
		Definition: fDef,
	}

	body, err := json.Marshal(fedUp)
	if err != nil {
		return nil, err
	}

	//body = []byte(`{"value":{"uri":"amqp://ares_admin:yYaCJESJ0LsT@188.166.84.89:5672/58af15daecfb2c1b3e27a7ac", "exchange":"wutt"}}`)

	logger.Warn(string(body))
	req, err := newRequestWithBody(c, "PUT", "parameters/federation-upstream/"+PathEscape(vhost)+"/"+PathEscape(upstreamName), body)
	if err != nil {
		return nil, err
	}

	res, err = executeRequest(c, req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

//
// DELETE /api/parameters/federation-upstream/{vhost}/{name}
//

// Deletes a federation upstream.
func (c *Client) DeleteFederationUpstream(vhost, upstreamName string) (res *http.Response, err error) {
	req, err := newRequestWithBody(c, "DELETE", "parameters/federation-upstream/"+PathEscape(vhost)+"/"+PathEscape(upstreamName), nil)

	if err != nil {
		return nil, err
	}

	res, err = executeRequest(c, req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

//
// GET /api/parameters/federation-upstream-set/{vhost}/{name}
//

// Gets a federation upstream set.
func (c *Client) GetFederationUpstreamSet(vhost, upstreamSetName string) (upstreamNames []string, err error) {
	req, err := newGETRequest(c, "parameters/federation-upstream-set/"+PathEscape(vhost)+"/"+PathEscape(upstreamSetName))

	if err != nil {
		fmt.Errorf("newGETRequest Error: %s\n", err)
		return nil, err
	}

	var rec *FederationUpstreamSet

	if err = executeAndParseRequest(c, req, &rec); err != nil {
		fmt.Errorf("executeAndParseRequest Error: %s\n", err)
		return nil, err
	}

	for _, upsDef := range rec.Definition {
		upstreamNames = append(upstreamNames, upsDef.Upstream)
	}

	return upstreamNames, nil

}

func (c *Client) ToFederationUpstreamSet(upstreamNames []string) (res FederationUpstreamSet) {
	fsDefs := []FederationSetDefinition{}

	for _, upstreamName := range upstreamNames {
		fsDefs = append(fsDefs, FederationSetDefinition{Upstream: upstreamName})
	}

	res = FederationUpstreamSet{
		Definition: fsDefs,
	}

	return res
}

//
//
// PUT /api/parameters/federation-upstream-set/{vhost}/{name}
//

// Puts a federation upstream set.
func (c *Client) PutFederationUpstreamSet(vhost, upstreamSetName string, upstreamNames []string) (res *http.Response, err error) {

	fedUpSet := c.ToFederationUpstreamSet(upstreamNames)

	body, err := json.Marshal(fedUpSet)

	if err != nil {
		return nil, err
	}

	req, err := newRequestWithBody(c, "PUT", "parameters/federation-upstream-set/"+PathEscape(vhost)+"/"+PathEscape(upstreamSetName), body)

	if err != nil {
		return nil, err
	}

	res, err = executeRequest(c, req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

//
// GET /api/parameters/federation-upstream-set/{vhost}/{name}
// PUT /api/parameters/federation-upstream-set/{vhost}/{name}
//

// Puts a federation upstream set.
func (c *Client) AddFederationUpstreamToSet(vhost, upstreamSetName string, upstreamName string) (res *http.Response, err error) {
	fed, err := c.GetFederationUpstreamSet(vhost, upstreamSetName)

	fmt.Errorf("------------------------------\n")

	if err != nil {
		if err.Error() == "not found" {
			res, err = c.PutFederationUpstreamSet(vhost, upstreamSetName, []string{upstreamName})
			return
		}
		return nil, err
	}

	fed = append(fed, upstreamName)

	res, err = c.PutFederationUpstreamSet(vhost, upstreamSetName, fed)

	if err != nil {
		err = errors.Wrap(err, "PutFederationUpstreamSet Error")

		return nil, err
	}

	return res, nil
}
