package hnclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
)

type (
	AlgoliaHit struct {
		ID         int      `json:"objectID,string"`
		Author     string   `json:"author"`
		Title      string   `json:"title"`
		Text       string   `json:"comment_text"`
		StoryTitle string   `json:"story_title"`
		CreatedAt  int64    `json:"created_at_i"`
		Tags       []string `json:"_tags"`
	}

	SearchResponse struct {
		Hits    []AlgoliaHit `json:"hits"`
		Page    int          `json:"page"`
		NbPages int          `json:"nbPages"`
	}
)

type Client struct {
	httpCli *http.Client
	apiURL  url.URL
}

func New(hc *http.Client) (*Client, error) {
	u, err := url.Parse("https://hn.algolia.com/api/v1/search_by_date")
	if err != nil {
		return nil, fmt.Errorf("could not parse URL: %w", err)
	}

	return &Client{
		httpCli: hc,
		apiURL:  *u,
	}, nil
}

func (c *Client) Search(ctx context.Context, query string, since int64) ([]AlgoliaHit, error) {
	var allHits []AlgoliaHit

	for page := 0; ; page++ {
		u := c.apiURL

		q := u.Query()
		q.Set("query", query)
		q.Set("tags", "(story,comment)")
		q.Set("numericFilters", fmt.Sprintf("created_at_i>%d", since))
		q.Set("hitsPerPage", "100")
		q.Set("restrictSearchableAttributes", "title,comment_text")
		q.Set("page", strconv.Itoa(page))

		u.RawQuery = q.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, fmt.Errorf("could not create request: %w", err)
		}

		resp, err := c.httpCli.Do(req)
		if err != nil {
			return nil, fmt.Errorf("could not send request: %w", err)
		}

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		var res SearchResponse
		if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("could not decode response: %w", err)
		}
		resp.Body.Close()

		if res.Page != page {
			return nil, fmt.Errorf("unexpected page in response: got %d, want %d", res.Page, page)
		}

		allHits = append(allHits, res.Hits...)

		if res.NbPages <= 0 || page >= res.NbPages-1 {
			break
		}
	}
	return allHits, nil
}
