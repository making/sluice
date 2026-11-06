;;;; A plugin in Lisp that adds a "sluice" field to a JSON object a response carries: the
;;;; route, the plugin, the request and the status the client relayed it for.
;;;;
;;;; Anything else, an encoded body or one that does not parse among them, goes on as it came.

(require :proxy-wasm "proxy-wasm.lisp")

(defpackage :json-plugin (:use :cl))

(in-package :json-plugin)

(defun json-p ()
  (let ((type (proxy-wasm:get-header :response "content-type")))
    (and type
         (search "application/json" type)
         (null (proxy-wasm:get-header :response "content-encoding")))))

;; The property is the status's little-endian bytes.
(defun status ()
  (let ((code (proxy-wasm:get-property-octets "response" "code")))
    (+ (aref code 0) (* 256 (aref code 1)))))

(defun metadata ()
  (let ((fields (make-hash-table :test 'equal)))
    (setf (gethash "route" fields) (proxy-wasm:get-property "route_name"))
    (setf (gethash "plugin" fields) (proxy-wasm:get-property "plugin_name"))
    (setf (gethash "method" fields) (proxy-wasm:get-property "request" "method"))
    (setf (gethash "path" fields) (proxy-wasm:get-property "request" "path"))
    (setf (gethash "status" fields) (status))
    fields))

(proxy-wasm:define-filter
  (:on-response-headers (stream num-headers end-of-stream)
    ;; the body changes length: the client frames it anew
    (when (json-p)
      (proxy-wasm:remove-header :response "content-length"))
    :continue)
  (:on-response-body (stream body-size end-of-stream)
    (cond ((not (json-p)) :continue)
          ;; held until the whole of it is here
          ((not end-of-stream) :pause)
          (t
           (let ((document
                  (ignore-errors (rontolisp:json-parse (proxy-wasm:get-buffer :response-body)))))
             (when (hash-table-p document)
               (setf (gethash "sluice" document) (metadata))
               (proxy-wasm:set-buffer :response-body (rontolisp:json-stringify document))))
           :continue))))
