/********************************************************************

  Copyright 2012 Konstantin Olkhovskiy <lupus@oxnull.net>

  This file is part of Mersenne.

  Mersenne is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  any later version.

  Mersenne is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with Mersenne.  If not, see <http://www.gnu.org/licenses/>.

 ********************************************************************/
#include <err.h>
#include <assert.h>
#include <netinet/tcp.h>
#include <errno.h>

#include <uthash.h>
#include "http_parser.h"
#include "kstring.h"
#include "ccan-json.h"

#include <mersenne/htstatus.h>
#include <mersenne/context.h>
#include <mersenne/util.h>

#include <asset_registry.h>

struct connection_fiber_arg {
	int fd;
	struct sockaddr_in addr;
	struct path_handler *handlers;
};

struct parser_arg {
	struct fbr_context *fctx;
	int fd;
	kstring_t url;
	struct path_handler *handlers;
	int finish;
};

struct request_info {
	FILE *ms;
	struct http_parser *parser;
	struct http_parser_url *url;
};

enum handler_type {
	HT_STATIC,
	HT_DYNAMIC,
};

struct static_path_handler {
	const char *ptr;
	size_t size;
};

struct dynamic_path_handler {
	void (*hfunc)(ME_P_ struct request_info *r);
};

struct path_handler {
	const char *path;
	const char *mime_type;
	enum handler_type htype;
	union {
		struct static_path_handler static_handler;
		struct dynamic_path_handler dynamic_handler;
	};
	UT_hash_handle hh;
};

static void api_status_hfunc(ME_P_ struct request_info *r)
{
	JsonNode *obj = json_mkobject();
	json_append_member(obj, "ip",
			json_mkstring(inet_ntoa(mctx->me->addr.sin_addr)));
	json_append_member(obj, "peer_num",
			json_mknumber(mctx->me->index));
	fprintf(r->ms, "%s", json_encode(obj));
	json_delete(obj);
}

#define json_option(_name_, _var_name_)                                       \
	if (args_info->_var_name_##_given) {                                  \
		opt_obj = json_mkobject();                                    \
		json_append_member(opt_obj, "name", json_mkstring(_name_));   \
		json_append_member(opt_obj, "value",                          \
				json_mkstring(args_info->_var_name_##_orig)); \
		json_append_element(arr, opt_obj);                            \
	}

#define json_flag(_name_, _var_name_)                                       \
	if (args_info->_var_name_##_given) {                                \
		opt_obj = json_mkobject();                                  \
		json_append_member(opt_obj, "name", json_mkstring(_name_)); \
		json_append_member(opt_obj, "value",                        \
				json_mkbool(args_info->_var_name_##_flag)); \
		json_append_element(arr, opt_obj);                          \
	}

static void api_config_hfunc(ME_P_ struct request_info *r)
{
	struct gengetopt_args_info *args_info = &mctx->args_info;
	JsonNode *arr = json_mkarray();
	JsonNode *opt_obj;
	json_option("peer-number", peer_number)
	json_option("client-port", client_port)
	json_option("port-num", port_num)
	json_option("log-level", log_level)
	json_option("max-instances", max_instances)
	json_option("leader-delta", leader_delta)
	json_option("leader-epsilon", leader_epsilon)
	json_option("proposer-instance-window", proposer_instance_window)
	json_option("proposer-timeout-1", proposer_timeout_1)
	json_option("proposer-timeout-2", proposer_timeout_2)
	json_option("proposer-queue-size", proposer_queue_size)
	json_option("learner-instance-window", learner_instance_window)
	json_option("learner-retransmit-age", learner_retransmit_age)
	json_option("acceptor-repeat-interval", acceptor_repeat_interval)
	json_option("db-dir", db_dir)
	json_option("ldb-write-buffer", ldb_write_buffer)
	json_option("ldb-max-open-files", ldb_max_open_files)
	json_option("ldb-cache", ldb_cache)
	json_option("ldb-block-size", ldb_block_size)
	json_flag("ldb-compression", ldb_compression);
	json_option("acceptor-truncate", acceptor_truncate)
	json_flag("wait-for-debugger", wait_for_debugger);
	json_option("statd-ip", statd_ip)
	json_option("statd-port", statd_port)
	fprintf(r->ms, "%s", json_encode(arr));
	json_delete(arr);
}


int on_headers_complete(http_parser *parser)
{
	struct parser_arg *arg = parser->data;

	arg->finish = 1;
	return 0;
}

int on_url(http_parser *parser, const char *at, size_t length)
{
	struct parser_arg *arg = parser->data;
	kputsn(at, length, &arg->url);
	return 0;
}

static const char response_400[] =
	"HTTP/1.0 400 Bad Request\r\n"
	"\r\n";

static const char response_404[] =
	"HTTP/1.0 404 Not Found\r\n"
	"\r\n"
	"<!DOCTYPE HTML PUBLIC \"-//IETF//DTD HTML 2.0//EN\">"
	"<html><head>"
	"<title>404 Not Found</title>"
	"</head><body>"
	"<h1>Not Found</h1>"
	"<p>The requested URL was not found on this server.</p>"
	"</body></html>";

static void connection_fiber(struct fbr_context *fctx, void *_arg)
{
	struct me_context *mctx;
	struct connection_fiber_arg *arg_p = _arg;
	struct connection_fiber_arg arg = *arg_p;
	int fd = arg.fd;
	http_parser_settings settings;
	http_parser parser;
	char buf[BUFSIZ];
	size_t nparsed;
	ssize_t retval;
	struct parser_arg parg;
	char *ptr;
	size_t size;
	FILE *ms;
	struct request_info info;
	struct http_parser_url url;
	struct path_handler *handler;
	char path[256];
	kstring_t resp = {0, 0, NULL};

	mctx = container_of(fctx, struct me_context, fbr);
	memset(&settings, 0x00, sizeof(settings));
	settings.on_url = on_url;
	settings.on_headers_complete = on_headers_complete;
	http_parser_init(&parser, HTTP_REQUEST);
	memset(&parg, 0x00, sizeof(parg));
	parg.fctx = fctx;
	parg.fd = fd;
	parg.finish = 0;
	parg.handlers = arg.handlers;
	parser.data = &parg;

	for (;;) {
		retval = fbr_read(fctx, fd, buf, sizeof(buf));
		if (0 > retval) {
			fbr_log_w(&mctx->fbr, "fbr_read failed: %s",
					strerror(errno));
			goto terminate;
		}
		if (0 == retval)
			break;

		nparsed = http_parser_execute(&parser, &settings, buf, retval);

		if (nparsed != retval) {
			fbr_log_w(&mctx->fbr, "error parsing HTTP request");
			goto resp_400;
		}

		if (!parg.finish)
			continue;

		retval = http_parser_parse_url(ks_str(&parg.url),
				ks_len(&parg.url), 0, &url);
		info.parser = &parser;
		info.url = &url;

		if (0 == (url.field_set & (1 << UF_PATH))) {
			fbr_log_w(&mctx->fbr, "no path info in url");
			goto resp_400;
		}

		if (url.field_data[UF_PATH].len > sizeof(path) - 1) {
			fbr_log_w(&mctx->fbr, "path is too long");
			goto resp_400;
		}

		memcpy(path, ks_str(&parg.url) + url.field_data[UF_PATH].off,
				url.field_data[UF_PATH].len);
		path[url.field_data[UF_PATH].len] = '\0';
		fbr_log_d(&mctx->fbr, "path: %s", path);

		HASH_FIND_STR(arg.handlers, path, handler);
		if (!handler) {
			fbr_log_i(&mctx->fbr, "no handler found for `%s`", path);
			goto resp_404;
		}

		ksprintf(&resp, "HTTP/1.0 200 OK\r\n");
		ksprintf(&resp, "Content-Type: %s\r\n",	handler->mime_type);
		if (HT_DYNAMIC == handler->htype) {
			ms = open_memstream(&ptr, &size);
			info.ms = ms;
			handler->dynamic_handler.hfunc(ME_A_ &info);
			fflush(ms);
			ksprintf(&resp, "Content-Length: %zd\r\n", size);
			ksprintf(&resp, "\r\n");
			retval = fbr_write_all(&mctx->fbr, fd, ks_str(&resp),
					ks_len(&resp));
			if (retval != ks_len(&resp)) {
				fbr_log_w(&mctx->fbr,
						"fbr_write_all failed: %s",
						strerror(errno));
				fclose(ms);
				free(ptr);
				goto terminate;
			}
			retval = fbr_write_all(&mctx->fbr, fd, ptr, size);
			if (retval != size) {
				fbr_log_w(&mctx->fbr,
						"fbr_write_all failed: %s",
						strerror(errno));
				fclose(ms);
				free(ptr);
				goto terminate;
			}
			fclose(ms);
			free(ptr);
		} else if (HT_STATIC == handler->htype) {
			ksprintf(&resp, "Content-Length: %zd\r\n",
					handler->static_handler.size);
			ksprintf(&resp, "\r\n");
			retval = fbr_write_all(&mctx->fbr, fd, ks_str(&resp),
					ks_len(&resp));
			if (retval != ks_len(&resp)) {
				fbr_log_w(&mctx->fbr,
						"fbr_write_all failed: %s",
						strerror(errno));
				goto terminate;
			}
			retval = fbr_write_all(&mctx->fbr, fd,
					handler->static_handler.ptr,
					handler->static_handler.size);
			if (retval != handler->static_handler.size) {
				fbr_log_w(&mctx->fbr,
						"fbr_write_all failed: %s",
						strerror(errno));
				goto terminate;
			}
		} else {
			errx(EXIT_FAILURE, "invalid handler type: %d",
					handler->htype);
		}
	}

	goto terminate;

resp_404:
	fbr_write_all(&mctx->fbr, arg.fd, response_404, strlen(response_404));
	goto terminate;

resp_400:
	fbr_write_all(&mctx->fbr, arg.fd, response_400, strlen(response_400));

terminate:
	fbr_log_d(&mctx->fbr, "closing htstatus connection for fd %d", fd);
	shutdown(fd, SHUT_RDWR);
	close(fd);
}

#define api_handler(_path_, _func_)                                  \
		handler = malloc(sizeof(*handler));                  \
		handler->htype = HT_DYNAMIC;                         \
		handler->path = _path_;                              \
		handler->mime_type = "application/json";             \
		handler->dynamic_handler.hfunc = _func_;             \
		fbr_log_d(&mctx->fbr,                                \
				"registered dynamic handler for %s", \
				handler->path);                      \
		HASH_ADD_KEYPTR(hh, handlers, handler->path,         \
				strlen(handler->path), handler);

void me_htstatus_fiber(struct fbr_context *fiber_context, void *_arg)
{
	struct me_context *mctx;
	int sockfd;
	fbr_id_t fiber;
	struct connection_fiber_arg arg;
	socklen_t addrlen = sizeof(arg.addr);
	unsigned i;
	struct path_handler *handlers = NULL;
	struct path_handler *handler, *x;

	mctx = container_of(fiber_context, struct me_context, fbr);

	for (i = 0; i < assets_len; i++) {
		handler = malloc(sizeof(*handler));
		handler->htype = HT_STATIC;
		handler->path = assets[i].orig_filename;
		handler->mime_type = assets[i].mime_type;
		handler->static_handler.ptr = assets[i].content_ptr;
		handler->static_handler.size = *assets[i].length_ptr;
		fbr_log_d(&mctx->fbr,
				"registered static handler for %s, length %zd",
				handler->path, handler->static_handler.size);
		HASH_ADD_KEYPTR(hh, handlers, handler->path,
				strlen(handler->path), handler);
		if (!strcmp("/index.html", handler->path)) {
			x = malloc(sizeof(*x));
			memcpy(x, handler, sizeof(*x));
			HASH_ADD_KEYPTR(hh, handlers, "/", strlen("/"),
					x);
		}
	}

	api_handler("/api/status", api_status_hfunc);
	api_handler("/api/config", api_config_hfunc);

	arg.handlers = handlers;

	for (;;) {
		sockfd = fbr_accept(&mctx->fbr, mctx->htstatus_fd,
				(struct	sockaddr *)&arg.addr, &addrlen);
		if (-1 == sockfd)
			err(EXIT_FAILURE, "fbr_accept failed on tcp socket");
		arg.fd = sockfd;
		fbr_log_d(&mctx->fbr, "accepted htstatus socket %d", sockfd);
		fiber = fbr_create(&mctx->fbr, "htstatus_client",
				connection_fiber, &arg, 0);
		fbr_transfer(&mctx->fbr, fiber);
	}
}
