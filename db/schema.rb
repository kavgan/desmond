# encoding: UTF-8
# This file is auto-generated from the current state of the database. Instead
# of editing this file, please use the migrations feature of Active Record to
# incrementally modify your database, and then regenerate this schema definition.
#
# Note that this schema.rb definition is the authoritative source for your
# database schema. If you need to create the application database on another
# system, you should be using db:schema:load, not running all the migrations
# from scratch. The latter is a flawed and unsustainable approach (the more migrations
# you'll amass, the slower it'll run and the greater likelihood for issues).
#
# It's strongly recommended that you check this file into your version control system.

ActiveRecord::Schema.define(version: 1) do

  # These are extensions that must be enabled in order to support this database
  enable_extension "plpgsql"

  create_table "desmond_job_runs", force: :cascade do |t|
    t.string   "job_id",       null: false
    t.string   "job_class",    null: false
    t.string   "user_id",      null: false
    t.string   "status",       null: false
    t.datetime "queued_at",    null: false
    t.datetime "executed_at"
    t.datetime "completed_at"
    t.json     "details",      null: false
  end

  add_index "desmond_job_runs", ["job_id"], name: "index_desmond_job_runs_on_job_id", using: :btree
  add_index "desmond_job_runs", ["user_id"], name: "index_desmond_job_runs_on_user_id", using: :btree

  create_table "que_jobs", primary_key: "queue", force: :cascade do |t|
    t.integer  "priority",    limit: 2, default: 100,                                        null: false
    t.datetime "run_at",                default: "now()",                                    null: false
    t.integer  "job_id",      limit: 8, default: "nextval('que_jobs_job_id_seq'::regclass)", null: false
    t.text     "job_class",                                                                  null: false
    t.json     "args",                  default: [],                                         null: false
    t.integer  "error_count",           default: 0,                                          null: false
    t.text     "last_error"
  end

end
