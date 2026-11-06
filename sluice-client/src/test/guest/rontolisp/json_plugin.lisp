;;;; A plugin in Lisp that adds a "sluice" field to a JSON object a response carries: the
;;;; route, the plugin, the request and the status the client relayed it for.
;;;;
;;;; The body stays where the host put it: rontolisp:json-span checks that it is JSON and
;;;; finds the object's closing brace, and the field goes in before it. Nothing of the body
;;;; becomes a Lisp value. Anything else, an encoded body or one that is no JSON object
;;;; among them, goes on as it came. A request asks for no encoding, so an upstream that
;;;; would compress sends what the plugin can read.

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
  (:on-request-headers (stream num-headers end-of-stream)
    (proxy-wasm:remove-header :request "accept-encoding")
    :continue)
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
           (proxy-wasm:with-buffer (address size :response-body 0 body-size)
             ;; the document, its members when it is a container; NIL when it is no
             ;; JSON (not IGNORE-ERRORS, whose second value is the condition)
             (multiple-value-bind (start end members)
                 (handler-case (rontolisp:json-span address size) (error () nil))
               (when (and members (eql (proxy-wasm:load8 (+ address start)) 123))
                 ;; before the closing brace, a comma after a member
                 (proxy-wasm:set-buffer :response-body
                                        (concatenate 'string (if (plusp members) "," "")
                                                     "\"sluice\":"
                                                     (rontolisp:json-stringify (metadata)))
                                        :start (- end 1)
                                        :size 0))))
           :continue))))
