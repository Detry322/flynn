package main

import (
	"encoding/json"
	"fmt"
	"time"

	. "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
	cc "github.com/flynn/flynn/controller/client"
	ct "github.com/flynn/flynn/controller/types"
)

func (s *S) TestEvents(c *C) {
	app1 := s.createTestApp(c, &ct.App{Name: "app1"})
	app2 := s.createTestApp(c, &ct.App{Name: "app2"})
	release := s.createTestRelease(c, &ct.Release{})

	jobID1 := "host-job1"
	jobID2 := "host-job2"
	jobID3 := "host-job3"
	jobs := []*ct.Job{
		{ID: jobID1, AppID: app1.ID, ReleaseID: release.ID, Type: "web", State: "starting"},
		{ID: jobID1, AppID: app1.ID, ReleaseID: release.ID, Type: "web", State: "up"},
		{ID: jobID2, AppID: app1.ID, ReleaseID: release.ID, Type: "web", State: "starting"},
		{ID: jobID2, AppID: app1.ID, ReleaseID: release.ID, Type: "web", State: "up"},
		{ID: jobID3, AppID: app2.ID, ReleaseID: release.ID, Type: "web", State: "starting"},
		{ID: jobID3, AppID: app2.ID, ReleaseID: release.ID, Type: "web", State: "up"},
	}

	listener := newEventListener(&EventRepo{db: s.hc.db})
	c.Assert(listener.Listen(), IsNil)

	// sub1 should receive job events for app1, job1
	sub1, err := listener.Subscribe(app1.ID, []string{string(ct.EventTypeJob)}, jobID1)
	c.Assert(err, IsNil)
	defer sub1.Close()

	// sub2 should receive all job events for app1
	sub2, err := listener.Subscribe(app1.ID, []string{string(ct.EventTypeJob)}, "")
	c.Assert(err, IsNil)
	defer sub2.Close()

	// sub3 should receive all job events for app2
	sub3, err := listener.Subscribe(app2.ID, []string{}, "")
	c.Assert(err, IsNil)
	defer sub3.Close()

	for _, job := range jobs {
		s.createTestJob(c, job)
	}

	assertJobEvents := func(sub *EventSubscriber, expected []*ct.Job) {
		var index int
		for {
			select {
			case e, ok := <-sub.Events:
				if !ok {
					c.Fatalf("unexpected close of event stream: %s", sub.Err)
				}
				var jobEvent ct.JobEvent
				c.Assert(json.Unmarshal(e.Data, &jobEvent), IsNil)
				job := expected[index]
				c.Assert(jobEvent, DeepEquals, ct.JobEvent{
					JobID:     job.ID,
					AppID:     job.AppID,
					ReleaseID: job.ReleaseID,
					Type:      job.Type,
					State:     job.State,
				})
				index += 1
				if index == len(expected) {
					return
				}
			case <-time.After(time.Second):
				c.Fatal("timed out waiting for app event")
			}
		}
	}
	assertJobEvents(sub1, jobs[0:2])
	assertJobEvents(sub2, jobs[0:4])
	assertJobEvents(sub3, jobs[4:6])
}

func (s *S) TestStreamEventsWithoutApp(c *C) {
	events := make(chan *ct.Event)
	stream, err := s.c.StreamEvents(cc.StreamEventsOptions{}, events)
	c.Assert(err, IsNil)
	defer stream.Close()

	// send fake event

	// TODO(jvatic): Use a real resource
	type FakeEvent struct {
		Message string `json:"message"`
	}

	createEvent := func(e FakeEvent) {
		data, err := json.Marshal(e)
		c.Assert(err, IsNil)
		query := "INSERT INTO events (object_id, object_type, data) VALUES ($1, $2, $3)"
		c.Assert(s.hc.db.Exec(query, "fake-id", string(ct.EventTypeScale), data), IsNil)
	}
	createEvent(FakeEvent{Message: "payload"})

	select {
	case e, ok := <-events:
		if !ok {
			c.Fatal("unexpected close of event stream")
		}
		var event FakeEvent
		c.Assert(json.Unmarshal(e.Data, &event), IsNil)
		c.Assert(event.Message, Equals, "payload")
	case <-time.After(time.Second):
		c.Fatal("Timed out waiting for event")
	}
}

func (s *S) TestStreamAppLifeCycleEvents(c *C) {
	release := s.createTestRelease(c, &ct.Release{})

	events := make(chan *ct.Event)
	stream, err := s.c.StreamEvents(cc.StreamEventsOptions{}, events)
	c.Assert(err, IsNil)
	defer stream.Close()

	app := s.createTestApp(c, &ct.App{Name: "app3"})

	c.Assert(s.c.SetAppRelease(app.ID, release.ID), IsNil)
	newStrategy := "one-by-one"
	c.Assert(s.c.UpdateApp(&ct.App{
		ID:       app.ID,
		Strategy: newStrategy,
	}), IsNil)
	newMeta := map[string]string{
		"foo": "bar",
	}
	c.Assert(s.c.UpdateApp(&ct.App{
		ID:   app.ID,
		Meta: newMeta,
	}), IsNil)

	eventAssertions := []func(*ct.AppEvent){
		func(e *ct.AppEvent) {
			c.Assert(e.Type, Equals, "create")
			c.Assert(e.App.ReleaseID, Equals, app.ReleaseID)
			c.Assert(e.App.Strategy, Equals, app.Strategy)
			c.Assert(e.App.Meta, DeepEquals, app.Meta)
		},
		func(e *ct.AppEvent) {
			c.Assert(e.Type, Equals, "update")
			c.Assert(e.App.ReleaseID, Equals, release.ID)
			c.Assert(e.App.Strategy, Equals, app.Strategy)
			c.Assert(e.App.Meta, DeepEquals, app.Meta)
		},
		func(e *ct.AppEvent) {
			c.Assert(e.Type, Equals, "update")
			c.Assert(e.App.Strategy, Equals, newStrategy)
			c.Assert(e.App.Meta, DeepEquals, newMeta)
		},
	}

	for i, fn := range eventAssertions {
		select {
		case e, ok := <-events:
			if !ok {
				c.Fatal("unexpected close of event stream")
			}
			var appEvent *ct.AppEvent
			c.Assert(json.Unmarshal(e.Data, &appEvent), IsNil)
			c.Assert(e.AppID, Equals, app.ID)
			c.Assert(e.ObjectType, Equals, ct.EventTypeApp)
			c.Assert(e.ObjectID, Equals, app.ID)
			c.Assert(appEvent.App, NotNil)
			c.Assert(appEvent.App.ID, Equals, app.ID)
			fn(appEvent)
		case <-time.After(time.Second):
			c.Fatal(fmt.Sprintf("Timed out waiting for event %d", i))
		}
	}
}
