#include "api.h"

#include "src/common/constants.h"
#include "src/common/protocol.h"

#include <fcntl.h>
#include <errno.h>
#include <stdio.h>

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path,
                int *notif_pipe) {
    // Criar FIFO de pedidos
    if (mkfifo(req_pipe_path, 0640) == -1 && errno != EEXIST) {
        perror("[ERR]: Failed to create request FIFO");
        return 1;
    }

    // Criar FIFO de respostas
    if (mkfifo(resp_pipe_path, 0640) == -1 && errno != EEXIST) {
        perror("[ERR]: Failed to create response FIFO");
        unlink(req_pipe_path);
        return 1;
    }

    // Criar FIFO de notificações
    if (mkfifo(notif_pipe_path, 0640) == -1 && errno != EEXIST) {
        perror("[ERR]: Failed to create notification FIFO");
        unlink(req_pipe_path);
        unlink(resp_pipe_path);
        return 1;
    }

    // Abrir FIFO de registro do servidor
    int server_fd = open(server_pipe_path, O_WRONLY);
    if (server_fd == -1) {
        perror("[ERR]: Failed to open server FIFO");
        unlink(req_pipe_path);
        unlink(resp_pipe_path);
        unlink(notif_pipe_path);
        return 1;
    }

    // Formatar mensagem de conexão
    char buffer[BUFFER_SIZE];
    int written = snprintf(buffer, BUFFER_SIZE, "%s %s %s", 
                           req_pipe_path, resp_pipe_path, notif_pipe_path);
    if (written < 0 || written >= BUFFER_SIZE) {
        fprintf(stderr, "[ERR]: Failed to format connection message.\n");
        close(server_fd);
        unlink(req_pipe_path);
        unlink(resp_pipe_path);
        unlink(notif_pipe_path);
        return 1;
    }

    // Enviar mensagem ao servidor
    if (write(server_fd, buffer, written) == -1) {
        perror("[ERR]: Failed to write to server FIFO");
        close(server_fd);
        unlink(req_pipe_path);
        unlink(resp_pipe_path);
        unlink(notif_pipe_path);
        return 1; 
    }
    close(server_fd); // Fechar o FIFO do servidor após enviar o pedido

    // Abrir FIFO de notificações em modo não bloqueante
    *notif_pipe = open(notif_pipe_path, O_RDONLY | O_NONBLOCK);
    if (*notif_pipe == -1) {
        perror("[ERR]: Failed to open notification FIFO");
        unlink(req_pipe_path);
        unlink(resp_pipe_path);
        unlink(notif_pipe_path);
        return 1;
    }

    printf("[INFO]: Connected to server successfully.\n");
    return 0;
}


int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  return 0;
}

int kvs_subscribe(const char *key) {
  // send subscribe message to request pipe and wait for response in response
  // pipe
  return 0;
}

int kvs_unsubscribe(const char *key) {
  // send unsubscribe message to request pipe and wait for response in response
  // pipe
  return 0;
}
