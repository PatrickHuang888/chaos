package node

import (
	"fmt"
	"log"
	"net/http"
	"github.com/gorilla/mux"
	"github.com/siddontang/chaos/pkg/core"
	"github.com/unrolled/render"
)

type dbHandler struct {
	agent *Agent
	rd    *render.Render
}

func newDBHanlder(agent *Agent, rd *render.Render) *dbHandler {
	return &dbHandler{
		agent: agent,
		rd:    rd,
	}
}

func (h *dbHandler) getDB(w http.ResponseWriter, vars map[string]string) core.DB {
	name := vars["name"]
	db := core.GetDB(name)
	if db == nil {
		h.rd.JSON(w, http.StatusNotFound, fmt.Sprintf("db %s is not registered", name))
		return nil
	}
	return db
}

func (h *dbHandler) SetUp(w http.ResponseWriter, r *http.Request) {
	h.agent.dbLock.Lock()
	defer h.agent.dbLock.Unlock()

	vars := mux.Vars(r)
	db := h.getDB(w, vars)
	if db == nil {
		return
	}
	//node := r.FormValue("node")
	//nodes := strings.Split(r.FormValue("nodes"), ",")

	log.Printf("set up db %s on node %s", db.Name(), db.Node())
	if err := db.SetUp(h.agent.ctx); err != nil {
		log.Panicf("set up db %s failed %v", db.Name(), err)
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *dbHandler) Start(w http.ResponseWriter, r *http.Request) {
	h.agent.dbLock.Lock()
	defer h.agent.dbLock.Unlock()

	vars := mux.Vars(r)
	db := h.getDB(w, vars)
	if db == nil {
		return
	}

	service := r.FormValue("service")

	node:=db.Node()
	log.Printf("start service %s on node %s", service, node)
	if err := db.Start(h.agent.ctx, service); err != nil {
		log.Panicf("start service %s failed on %s with %v", service, node, err)
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.JSON(w, http.StatusOK, nil)
}

func (h *dbHandler) TearDown(w http.ResponseWriter, r *http.Request) {
	h.agent.dbLock.Lock()
	defer h.agent.dbLock.Unlock()

	vars := mux.Vars(r)
	db := h.getDB(w, vars)
	if db == nil {
		return
	}

	//node := r.FormValue("node")
	//nodes := strings.Split(r.FormValue("nodes"), ",")

	log.Printf("tear down db %s on node %s", db.Name(), db.Node())
	if err := db.TearDown(h.agent.ctx); err != nil {
		log.Panicf("tear down db %s failed %v", db.Name(), err)
		h.rd.JSON(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.rd.JSON(w, http.StatusOK, nil)
}
